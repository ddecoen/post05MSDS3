package post05

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Connection details
var (
	Hostname2 = ""
	Port2     = 2345
	Username = ""
	Password2 = ""
	Database2 = ""
)

// MSDSdata is for holding full MSDS data
// MSDSCourseCatalog table + MSDS
type MSDSCourseCatalog struct {
	ID          int 
	Username    string
	CID         string `json:"couseI_D"`
	CNAME	    string `json:"course_name"`
	CPREREQ     string `json:"prerequisite"`
}

func openConnection2() (*sql.DB, error) {
	// connection string
	conn2 := fmt.Sprintf("host=%s port=%d Course=%s password=%s dbname=%s sslmode=disable",
		Hostname2, Port2, Username, Password2, Database2)

	// open database
	db2, err := sql.Open("postgres", conn2)
	if err != nil {
		return nil, err
	}
	return db2, nil
}

// The function returns the User ID of the username
// -1 if the User does not exist
func exists2(username string) int {
	username = strings.ToLower(username)

	db2, err := openConnection2()
	if err != nil {
		fmt.Println(err)
		return -1
	}
	defer db2.Close()

	userID := -1
	statement := fmt.Sprintf(`SELECT "id" FROM "MSDS" where Username = '%s'`, username)
	rows, err := db2.Query(statement)

	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("Scan", err)
			return -1
		}
		userID = id
	}
	defer rows.Close()
	return userID
}

// AddCourse adds a new User to the database
// Returns new User ID
// -1 if there was an error
func AddUser2(d MSDSCourseCatalog) int {
	d.Username = strings.ToLower(d.Username)

	db2, err := openConnection2()
	if err != nil {
		fmt.Println(err)
		return -1
	}
	defer db2.Close()

	userID := exists2(d.Username)
	if userID != -1 {
		fmt.Println("User already exists:", Username)
		return -1
	}

	insertStatement := `insert into "MSDS" ("Username") values ($1)`
	_, err = db2.Exec(insertStatement, d.Username)
	if err != nil {
		fmt.Println(err)
		return -1
	}

	userID = exists2(d.Username)
	if userID == -1 {
		return userID
	}

	insertStatement = `insert into "MSDSCourseCatalog" ("CID", "CNAME", "CPREREQ")
	values ($1, $2, $3, $4)`
	_, err = db2.Exec(insertStatement, userID, d.CID, d.CNAME, d.CPREREQ)
	if err != nil {
		fmt.Println("db.Exec()", err)
		return -1
	}

	return userID
}

// DeleteUser deletes an existing User
func DeleteUser2(id int) error {
	db2, err := openConnection2()
	if err != nil {
		return err
	}
	defer db2.Close()

	// Does the ID exist?
	statement := fmt.Sprintf(`SELECT "Username" FROM "MSDS" where id = %d`, id)
	rows, err := db2.Query(statement)

	var username string
	for rows.Next() {
		err = rows.Scan(&username)
		if err != nil {
			return err
		}
	}
	defer rows.Close()

	if exists2(username) != id {
		return fmt.Errorf("User with ID %d does not exist", id)
	}

	// Delete from MSDSCourseCatalog
	deleteStatement := `delete from "MSDSCourseCatalog" where userid=$1`
	_, err = db2.Exec(deleteStatement, id)
	if err != nil {
		return err
	}

	// Delete from MSDS
	deleteStatement = `delete from "MSDS" where id=$1`
	_, err = db2.Exec(deleteStatement, id)
	if err != nil {
		return err
	}

	return nil
}

// ListUsers lists all Users in the database
func ListUsers2() ([]MSDSCourseCatalog, error) {
	Data := []MSDSCourseCatalog{}
	db2, err := openConnection2()
	if err != nil {
		return Data, err
	}
	defer db2.Close()

	rows, err := db2.Query(`SELECT "id","username","cid","cname","cprereq"
		FROM "MSDS","MSDSCourseCatalog"
		WHERE MSDS.id = MSDSCourseCatalog.userid`)
	if err != nil {
		return Data, err
	}

	for rows.Next() {
		var id int
		var username string
		var cid string
		var cname string
		var cprereq string
		err = rows.Scan(&id, &username, &cid, &cname, &cprereq)
		temp := MSDSCourseCatalog{ID: id, Username: username, CID: cid, CNAME: cname, CPREREQ: cprereq}
		Data = append(Data, temp)
		if err != nil {
			return Data, err
		}
	}
	defer rows.Close()
	return Data, nil
}

// UpdateUser is for updating an existing User
func UpdateUser2(d MSDSCourseCatalog) error {
	db2, err := openConnection2()
	if err != nil {
		return err
	}
	defer db2.Close()

	userID := exists2(d.Username)
	if userID == -1 {
		return errors.New("User does not exist")
	}
	d.ID = userID
	updateStatement := `update "MSDSCourseCatalog" set "cid"=$1, "cname"=$2, "cprereq"=$3 where "userid"=$4`
	_, err = db2.Exec(updateStatement, d.CID, d.CNAME, d.CPREREQ, d.ID)
	if err != nil {
		return err
	}

	return nil
}