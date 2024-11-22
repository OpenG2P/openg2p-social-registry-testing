package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

const (
	START_ID                = 37726268
	RECORDS_PER_FILE        = 100000
	TOTAL_RECORDS           = 10000000
	OUTPUT_FOLDER           = "data"
	TOTAL_MEMBERS_PER_GROUP = 4
)

var mandalDistricts []MandalDistrict

type MandalDistrict struct {
	Mandal   string
	District string
}

type GroupAssignment struct {
	GroupID  int
	Members  []int
	Mandal   string
	District string
}

func loadMandalDistricts(csvFilePath string) error {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Read header
	_, err = reader.Read()
	if err != nil {
		return err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(record) < 2 {
			continue
		}
		mandalDistricts = append(mandalDistricts, MandalDistrict{
			Mandal:   record[0],
			District: record[1],
		})
	}
	return nil
}

func processGroupAssignments(records []int, groupStartId *int) ([]GroupAssignment, error) {
	var groupAssignments []GroupAssignment

	totalMandals := len(mandalDistricts)
	if totalMandals == 0 {
		return nil, fmt.Errorf("No mandal-district mappings available")
	}

	for i := 0; i < len(records); i += TOTAL_MEMBERS_PER_GROUP {
		endIndex := i + TOTAL_MEMBERS_PER_GROUP
		if endIndex > len(records) {
			break
		}
		members := records[i:endIndex]
		groupId := *groupStartId
		*groupStartId++

		// Select a random mandal-district pair
		index := gofakeit.Number(0, totalMandals-1)
		mandalDistrict := mandalDistricts[index]

		groupAssignments = append(groupAssignments, GroupAssignment{
			GroupID:  groupId,
			Members:  members,
			Mandal:   mandalDistrict.Mandal,
			District: mandalDistrict.District,
		})
	}
	return groupAssignments, nil
}

func main() {
	gofakeit.Seed(43) // Seed for reproducibility

	// Ensure the output folder and subfolders exist
	ensureFolders()

	// Load mandal-district mappings
	err := loadMandalDistricts("g2p_mandal_district_mapping.csv")
	if err != nil {
		fmt.Println("Error loading mandal-district mappings:", err)
		return
	}

	currentId := START_ID
	groupStartId := START_ID + TOTAL_RECORDS
	fileCount := 1

	for currentId < START_ID+TOTAL_RECORDS {
		endId := currentId + RECORDS_PER_FILE
		if endId > START_ID+TOTAL_RECORDS {
			endId = START_ID + TOTAL_RECORDS
		}

		// Generate file names
		resPartnerFileName := filepath.Join(OUTPUT_FOLDER, "partners", fmt.Sprintf("res_partner_insert_%d.sql", fileCount))
		g2pRegIdFileName := filepath.Join(OUTPUT_FOLDER, "reg_ids", fmt.Sprintf("g2p_reg_id_insert_%d.sql", fileCount))
		groupFileName := filepath.Join(OUTPUT_FOLDER, "groups", fmt.Sprintf("res_partner_group_insert_%d.sql", fileCount))
		groupRegIdsFileName := filepath.Join(OUTPUT_FOLDER, "group_reg_ids", fmt.Sprintf("g2p_reg_id_group_insert_%d.sql", fileCount))
		groupMembershipFileName := filepath.Join(OUTPUT_FOLDER, "group_memberships", fmt.Sprintf("g2p_group_membership_insert_%d.sql", fileCount))

		records := []int{}
		for i := currentId; i < endId; i++ {
			records = append(records, i)
		}

		fmt.Printf("Generating records %d to %d\n", currentId, endId-1)
		fmt.Println("Total records generated:", len(records))

		// Process group assignments
		groupAssignments, err := processGroupAssignments(records, &groupStartId)
		if err != nil {
			fmt.Println("Error processing group assignments:", err)
			return
		}

		// Map partner IDs to their MandalDistrict
		partnerMandalDistrictMap := make(map[int]MandalDistrict)
		for _, ga := range groupAssignments {
			for _, memberId := range ga.Members {
				partnerMandalDistrictMap[memberId] = MandalDistrict{
					Mandal:   ga.Mandal,
					District: ga.District,
				}
			}
		}

		// For partners not in any group, assign random mandal and district
		for _, recordId := range records {
			if _, exists := partnerMandalDistrictMap[recordId]; !exists {
				index := gofakeit.Number(0, len(mandalDistricts)-1)
				mandalDistrict := mandalDistricts[index]
				partnerMandalDistrictMap[recordId] = mandalDistrict
			}
		}

		// Generate insert statements
		insertStatementResPartner := generateBulkInsertStatementResPartner(records, partnerMandalDistrictMap)
		insertStatementG2pRegId := generateBulkInsertStatementG2pRegId(records)

		// Write to files
		writeToFile(resPartnerFileName, insertStatementResPartner)
		writeToFile(g2pRegIdFileName, insertStatementG2pRegId)

		// Generate insert statements for groups
		if len(groupAssignments) > 0 {
			insertStatementGroup := generateBulkInsertStatementGroup(groupAssignments)
			insertStatementGroupMembership := generateBulkInsertStatementGroupMembership(groupAssignments)
			insertStatementG2pRegIdGroup := generateBulkInsertStatementGroupG2pRegId(groupAssignments)

			// Write the group files
			writeToFile(groupFileName, insertStatementGroup)
			writeToFile(groupMembershipFileName, insertStatementGroupMembership)
			writeToFile(groupRegIdsFileName, insertStatementG2pRegIdGroup)
		}

		currentId = endId
		fileCount++
	}
}

func ensureFolders() {
	folders := []string{
		filepath.Join(OUTPUT_FOLDER, "partners"),
		filepath.Join(OUTPUT_FOLDER, "reg_ids"),
		filepath.Join(OUTPUT_FOLDER, "groups"),
		filepath.Join(OUTPUT_FOLDER, "group_reg_ids"),
		filepath.Join(OUTPUT_FOLDER, "group_memberships"),
	}

	for _, folder := range folders {
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			err := os.MkdirAll(folder, os.ModePerm)
			if err != nil {
				fmt.Println("Error creating folder:", folder, err)
				return
			}
		}
	}
}

func writeToFile(fileName string, content string) {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error writing to file:", fileName, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(content)
	if err != nil {
		fmt.Println("Error writing content to file:", fileName, err)
		return
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing content to file:", fileName, err)
		return
	}
}

func escapeString(s string) string {
	return strings.Replace(s, "'", "''", -1)
}

func generateBulkInsertStatementResPartner(records []int, partnerMandalDistrictMap map[int]MandalDistrict) string {
	var values []string
	for _, recordId := range records {
		firstName := escapeString(gofakeit.FirstName())
		lastName := escapeString(gofakeit.LastName())
		createDate := gofakeit.DateRange(time.Now().AddDate(-10, 0, 0), time.Now())
		createDateStr := createDate.Format("2006-01-02 15:04:05")
		createDateDateStr := createDate.Format("2006-01-02")
		email := escapeString(gofakeit.Email())
		phone := escapeString(gofakeit.Phone())
		street := escapeString(gofakeit.Street())
		city := escapeString(gofakeit.City())
		zipCode := escapeString(gofakeit.Zip())
		companyName := escapeString(gofakeit.Company())
		website := escapeString(gofakeit.URL())
		isActive := true
		isRegistrant := true
		partnerShare := true
		isGroup := false
		dob := gofakeit.DateRange(time.Now().AddDate(-80, 0, 0), time.Now().AddDate(-20, 0, 0))
		dobStr := dob.Format("2006-01-02")
		typeOfDisability := gofakeit.RandomString([]string{"visual_impairment", "hearing_impairment", "physical_disability", "cognitive_disability"})
		casteEthnicGroup := gofakeit.RandomString([]string{"bantu", "nilotic", "afro_asian", "khoisan", "pygmy", "other"})
		belongToProtectedGroups := "yes"
		if !gofakeit.Bool() {
			belongToProtectedGroups = "no"
		}
		otherVulnerableStatus := "yes"
		if !gofakeit.Bool() {
			otherVulnerableStatus = "no"
		}
		educationLevel := gofakeit.RandomString([]string{"primary", "secondary", "higher_secondary", "bachelors", "masters"})
		employmentStatus := gofakeit.RandomString([]string{"employed_full", "employed_part", "self_employed", "unemployed"})
		maritalStatus := gofakeit.RandomString([]string{"single", "married", "divorced"})
		incomeSources := gofakeit.RandomString([]string{"agriculture", "business", "mining", "manufacturing", "construction"})
		annualIncome := gofakeit.RandomString([]string{"below_5000", "5001_10000", "above_10000"})
		ownsTwoWheeler := "yes"
		if !gofakeit.Bool() {
			ownsTwoWheeler = "no"
		}
		ownsThreeWheeler := "yes"
		if !gofakeit.Bool() {
			ownsThreeWheeler = "no"
		}
		ownsFourWheeler := "yes"
		if !gofakeit.Bool() {
			ownsFourWheeler = "no"
		}
		ownsCart := "yes"
		if !gofakeit.Bool() {
			ownsCart = "no"
		}
		landOwnership := "yes"
		if !gofakeit.Bool() {
			landOwnership = "no"
		}
		typeOfLandOwned := "None"
		if landOwnership == "yes" {
			typeOfLandOwned = gofakeit.RandomString([]string{"agricultural", "residential", "pastoral", "other"})
		}
		ownsHouse := "yes"
		if !gofakeit.Bool() {
			ownsHouse = "no"
		}
		ownsLivestock := "yes"
		if !gofakeit.Bool() {
			ownsLivestock = "no"
		}
		housingType := gofakeit.RandomString([]string{"permanent", "temporary"})
		houseCondition := gofakeit.RandomString([]string{"mud", "cement"})
		sanitationCondition := gofakeit.RandomString([]string{"yes", "no"})
		waterAccess := "yes"
		if !gofakeit.Bool() {
			waterAccess = "no"
		}
		electricityAccess := "yes"
		if !gofakeit.Bool() {
			electricityAccess = "no"
		}
		landSize := gofakeit.RandomString([]string{"100", "20", "312", "434", "513"})
		occupation := escapeString(gofakeit.RandomString([]string{
			"agricultural_worker", "artisan", "clerk", "domestic_worker", "driver",
			"fisherman", "forestry_worker", "manager", "professional", "sales_worker",
			"service_worker", "skilled_worker", "unskilled_worker", "other",
		}))
		name := escapeString(fmt.Sprintf("%s, %s", strings.ToUpper(lastName), strings.ToUpper(firstName)))
		// Get the mandal and district for this partner
		mandalDistrict := partnerMandalDistrictMap[recordId]

		valueStr := fmt.Sprintf("(%d, 1, '%s', '%s', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', NULL, 'en_US', 'UTC', NULL, NULL, '%s', 'Manager', 'contact', '%s', NULL, '%s', '%s', '%s', '%s', NULL, NULL, '%s', '%s', NULL, NULL, NULL, %t, NULL, FALSE, %t, '%s', NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', '%s', NULL, NULL, %t, %t, NULL, NULL, NULL, NULL, '%s', '%s', NULL, NULL, NULL, '%s', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')",
			recordId, createDateStr, name, name, website, street, zipCode, city, email, phone, companyName, createDateDateStr, isActive, partnerShare, createDateDateStr, occupation, createDateStr, isRegistrant, isGroup, lastName, firstName, dobStr, typeOfDisability, casteEthnicGroup, belongToProtectedGroups, otherVulnerableStatus, educationLevel, employmentStatus, maritalStatus, incomeSources, annualIncome, ownsTwoWheeler, ownsThreeWheeler, ownsFourWheeler, ownsCart, landOwnership, typeOfLandOwned, ownsHouse, ownsLivestock, housingType, houseCondition, sanitationCondition, waterAccess, electricityAccess, landSize, escapeString(mandalDistrict.Mandal), escapeString(mandalDistrict.District))
		values = append(values, valueStr)
	}

	return fmt.Sprintf(`
INSERT INTO res_partner (
    id, company_id, create_date, name, title, parent_id, user_id, state_id,
    country_id, industry_id, color, commercial_partner_id, create_uid, write_uid,
    complete_name, ref, lang, tz, vat, company_registry, website, function,
    type, street, street2, zip, city, email, phone, mobile, commercial_company_name,
    company_name, date, comment, partner_latitude, partner_longitude, active,
    employee, is_company, partner_share, write_date, message_bounce, email_normalized,
    signup_type, signup_expiration, signup_token, partner_gid, additional_info,
    phone_sanitized, disabled_by, civil_status, occupation, registration_date,
    address, disabled_reason, is_registrant, is_group, disabled, income, kind,
    is_partial_group, family_name, given_name, addl_name, birth_place, gender,
    birthdate, birthdate_not_exact, z_ind_grp_num_individuals, force_recompute_canary,
    region, ref_id, num_preg_lact_women, num_malnourished_children, num_disabled,
    type_of_disability, caste_ethnic_group, belong_to_protected_groups, other_vulnerable_status,
    education_level, employment_status, marital_status, income_sources, annual_income,
    owns_two_wheeler, owns_three_wheeler, owns_four_wheeler, owns_cart, land_ownership,
    type_of_land_owned, owns_house, owns_livestock, housing_type, house_condition,
    sanitation_condition, water_access, electricity_access, land_size, mandal, district
) VALUES %s;

SELECT setval('res_partner_id_seq', (SELECT MAX(id) FROM res_partner) + 1);
`, strings.Join(values, ",\n"))
}

func generateBulkInsertStatementG2pRegId(records []int) string {
	var values []string
	usedValues := make(map[string]bool)
	for _, partnerId := range records {
		idType := 2
		var value string
		for {
			value = gofakeit.Numerify("##########")
			if !usedValues[value] {
				usedValues[value] = true
				break
			}
		}
		status := "valid"
		valueStr := fmt.Sprintf("(%d, %d, NULL, NULL, '%s', '%s', NULL, NULL, NULL, NULL)", partnerId, idType, value, status)
		values = append(values, valueStr)
	}

	return fmt.Sprintf(`
INSERT INTO g2p_reg_id (
    partner_id, id_type, create_uid, write_uid, value, status, description,
    expiry_date, create_date, write_date
) VALUES %s;

SELECT setval('g2p_reg_id_id_seq', (SELECT MAX(id) FROM g2p_reg_id) + 1);
`, strings.Join(values, ",\n"))
}

func generateBulkInsertStatementGroupG2pRegId(groupAssignments []GroupAssignment) string {
	var values []string
	usedValues := make(map[string]bool)
	for _, ga := range groupAssignments {
		partnerId := ga.GroupID
		idType := 1 // 1 is family ID
		var value string
		for {
			value = gofakeit.Numerify("##########") // Unique 10-digit number
			if !usedValues[value] {
				usedValues[value] = true
				break
			}
		}
		status := "valid"
		valueStr := fmt.Sprintf("(%d, %d, NULL, NULL, '%s', '%s', NULL, NULL, NULL, NULL)", partnerId, idType, value, status)
		values = append(values, valueStr)
	}

	return fmt.Sprintf(`
INSERT INTO g2p_reg_id (
    partner_id, id_type, create_uid, write_uid, value, status, description,
    expiry_date, create_date, write_date
) VALUES %s;

SELECT setval('g2p_reg_id_id_seq', (SELECT MAX(id) FROM g2p_reg_id) + 1);
`, strings.Join(values, ",\n"))
}

func generateBulkInsertStatementGroup(groupAssignments []GroupAssignment) string {
	var values []string
	for _, ga := range groupAssignments {
		recordId := ga.GroupID
		mandal := escapeString(ga.Mandal)
		district := escapeString(ga.District)
		groupName := escapeString(gofakeit.Company()) + " Group" + strconv.Itoa(gofakeit.Number(1, 10000))
		createDate := gofakeit.DateRange(time.Now().AddDate(-10, 0, 0), time.Now())
		createDateStr := createDate.Format("2006-01-02 15:04:05")
		createDateDateStr := createDate.Format("2006-01-02")
		isActive := true
		isGroup := true
		isRegistrant := true
		partnerShare := true

		typeOfDisability := gofakeit.RandomString([]string{"visual_impairment", "hearing_impairment", "physical_disability", "cognitive_disability"})
		casteEthnicGroup := gofakeit.RandomString([]string{"bantu", "nilotic", "afro_asian", "khoisan", "pygmy", "other"})
		belongToProtectedGroups := "yes"
		if !gofakeit.Bool() {
			belongToProtectedGroups = "no"
		}
		otherVulnerableStatus := "yes"
		if !gofakeit.Bool() {
			otherVulnerableStatus = "no"
		}
		educationLevel := gofakeit.RandomString([]string{"primary", "secondary", "higher_secondary", "bachelors", "masters"})
		employmentStatus := gofakeit.RandomString([]string{"employed_full", "employed_part", "self_employed", "unemployed"})
		maritalStatus := gofakeit.RandomString([]string{"single", "married", "divorced"})
		incomeSources := gofakeit.RandomString([]string{"agriculture", "business", "mining", "manufacturing", "construction"})
		annualIncome := gofakeit.RandomString([]string{"below_5000", "5001_10000", "above_10000"})
		ownsTwoWheeler := "yes"
		if !gofakeit.Bool() {
			ownsTwoWheeler = "no"
		}
		ownsThreeWheeler := "yes"
		if !gofakeit.Bool() {
			ownsThreeWheeler = "no"
		}
		ownsFourWheeler := "yes"
		if !gofakeit.Bool() {
			ownsFourWheeler = "no"
		}
		ownsCart := "yes"
		if !gofakeit.Bool() {
			ownsCart = "no"
		}
		landOwnership := "yes"
		if !gofakeit.Bool() {
			landOwnership = "no"
		}
		typeOfLandOwned := "None"
		if landOwnership == "yes" {
			typeOfLandOwned = gofakeit.RandomString([]string{"agricultural", "residential", "pastoral", "other"})
		}
		ownsHouse := "yes"
		if !gofakeit.Bool() {
			ownsHouse = "no"
		}
		ownsLivestock := "yes"
		if !gofakeit.Bool() {
			ownsLivestock = "no"
		}
		housingType := gofakeit.RandomString([]string{"permanent", "temporary"})
		houseCondition := gofakeit.RandomString([]string{"mud", "cement"})
		sanitationCondition := gofakeit.RandomString([]string{"yes", "no"})
		waterAccess := "yes"
		if !gofakeit.Bool() {
			waterAccess = "no"
		}
		electricityAccess := "yes"
		if !gofakeit.Bool() {
			electricityAccess = "no"
		}
		landSize := strconv.Itoa(gofakeit.Number(1, 999))

		name := escapeString(groupName)

		valueStr := fmt.Sprintf("(%d, 1, '%s', '%s', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', NULL, 'en_US', 'UTC', NULL, NULL, NULL, 'Manager', 'contact', NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', '%s', '%s', NULL, NULL, NULL, %t, NULL, FALSE, %t, '%s', NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %t, %t, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s')",
			recordId, createDateStr, name, name, name, name, createDateDateStr, isActive, partnerShare, createDateDateStr, isRegistrant, isGroup, typeOfDisability, casteEthnicGroup, belongToProtectedGroups, otherVulnerableStatus, educationLevel, employmentStatus, maritalStatus, incomeSources, annualIncome, ownsTwoWheeler, ownsThreeWheeler, ownsFourWheeler, ownsCart, landOwnership, typeOfLandOwned, ownsHouse, ownsLivestock, housingType, houseCondition, sanitationCondition, waterAccess, electricityAccess, landSize, mandal, district)
		values = append(values, valueStr)
	}

	return fmt.Sprintf(`
INSERT INTO res_partner (
    id, company_id, create_date, name, title, parent_id, user_id, state_id,
    country_id, industry_id, color, commercial_partner_id, create_uid, write_uid,
    complete_name, ref, lang, tz, vat, company_registry, website, function,
    type, street, street2, zip, city, email, phone, mobile, commercial_company_name,
    company_name, date, comment, partner_latitude, partner_longitude, active,
    employee, is_company, partner_share, write_date, message_bounce, email_normalized,
    signup_type, signup_expiration, signup_token, partner_gid, additional_info,
    phone_sanitized, disabled_by, civil_status, occupation, registration_date,
    address, disabled_reason, is_registrant, is_group, disabled, income, kind,
    is_partial_group, family_name, given_name, addl_name, birth_place, gender,
    birthdate, birthdate_not_exact, z_ind_grp_num_individuals, force_recompute_canary,
    region, ref_id, num_preg_lact_women, num_malnourished_children, num_disabled,
    type_of_disability, caste_ethnic_group, belong_to_protected_groups, other_vulnerable_status,
    education_level, employment_status, marital_status, income_sources, annual_income,
    owns_two_wheeler, owns_three_wheeler, owns_four_wheeler, owns_cart, land_ownership,
    type_of_land_owned, owns_house, owns_livestock, housing_type, house_condition,
    sanitation_condition, water_access, electricity_access, land_size, mandal, district
) VALUES %s;

SELECT setval('res_partner_id_seq', (SELECT MAX(id) FROM res_partner) + 1);
`, strings.Join(values, ",\n"))
}

func generateBulkInsertStatementGroupMembership(groupAssignments []GroupAssignment) string {
	var values []string
	for _, ga := range groupAssignments {
		groupId := ga.GroupID
		memberIds := ga.Members
		createDate := gofakeit.DateRange(time.Now().AddDate(-10, 0, 0), time.Now())
		createDateStr := createDate.Format("2006-01-02 15:04:05")
		isEnded := false
		status := "active"

		for _, individualId := range memberIds {
			valueStr := fmt.Sprintf("(%d, %d, NULL, NULL, '%s', %t, '%s', NULL, '%s', '%s')",
				groupId, individualId, status, isEnded, createDateStr, createDateStr, createDateStr)
			values = append(values, valueStr)
		}
	}

	return fmt.Sprintf(`
INSERT INTO g2p_group_membership (
    "group", individual, create_uid, write_uid, status, is_ended, start_date, ended_date, create_date, write_date
) VALUES %s;

SELECT setval('g2p_group_membership_id_seq', (SELECT MAX(id) FROM g2p_group_membership) + 1);
`, strings.Join(values, ",\n"))
}
