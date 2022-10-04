package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/schema"
	_ "github.com/hashicorp/hcl"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/dosco/graphjin/core"
	_ "github.com/jackc/pgx/v4/stdlib"
)

type Request struct {
	Query     string
	Variables json.RawMessage
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Open a "connection" to sqlite.
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"), os.Getenv("PG_PORT"), os.Getenv("PG_USER"), os.Getenv("PG_PASSWORD"), os.Getenv("PG_DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("failed opening db: %s", err)
	}

	ctx := context.Background()

	// Open an atlas driver.
	driver, err := postgres.Open(db)
	if err != nil {
		log.Fatalf("failed opening atlas driver: %s", err)
	}

	existing, err := driver.InspectRealm(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Get the desired state from HCL.
	var desired schema.Realm

	h := `
schema "public" {}
table "users" {
	schema = schema.public
	column "id" {
		type    = uuid
		default = sql("gen_random_uuid()")
		unique = true
	}	
	column "created_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "updated_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "name" {
		type = text
	}	
	primary_key {
		columns = [column.id]
	}
}
table "groups" {
	schema = schema.public
	column "id" {
		type    = uuid
		default = sql("gen_random_uuid()")
		unique = true
	}	
	column "created_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "updated_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "name" {
		type = text
	}	
	primary_key {
		columns = [column.id]
	}
}
table "group_has_user" {
	schema = schema.public
	column "id" {
		type    = uuid
		default = sql("gen_random_uuid()")
		unique = true
	}
	column "created_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "updated_at" {
		type = timestamp
		default = sql("now()")
	}	
	column "user_id" {
		type = uuid
	}	
	column "group_id" {
		type = uuid
	}	
	primary_key {
		columns = [column.id]
	}
	foreign_key "users_kf" {
		columns = [column.user_id]
		ref_columns = [table.users.column.id]
	}
	foreign_key "groups_kf" {
		columns = [column.group_id]
		ref_columns = [table.groups.column.id]
	}
}
`

	err = postgres.EvalHCLBytes([]byte(h), &desired, nil)
	if err != nil {
		log.Fatal(err)
	}

	for _, item := range desired.Schemas {
		fmt.Println(item.Name)
		for _, table := range item.Tables {
			fmt.Println(table.Name)
		}
	}

	// Calculate the diff. The diff contains the drop table and column changes.
	diff, err := driver.RealmDiff(existing, &desired)
	if err != nil {
		log.Fatal(err)
	}

	/* 	// Keep only non-destructive operations.
	   	nonDestructive := filterDestructive(diff)

	   	// Create an SQL plan from the filtered changes.
	   	_, err = driver.PlanChanges(ctx, "change", nonDestructive)
	*/

	err = driver.ApplyChanges(ctx, diff)
	if err != nil {
		log.Fatal(err)
	}

	/* 	// Inspect the created table.
	   	sch, err := driver.InspectSchema(ctx, "public", &schema.InspectOptions{
	   		Tables: []string{"example"},
	   	})

	   	if err != nil {
	   		log.Fatalf("failed inspecting schema: %s", err)
	   	}
	*/
	config := &core.Config{
		Tables: []core.Table{
			{
				Columns: []core.Column{
					{},
				},
			},
		},
		/*
			 		RolesQuery: "SELECT * FROM users WHERE users.id = $user_id",
					Roles: []core.Role{
						{
							Tables: []core.RoleTable{
								{
									Name:   "users",
									Schema: "public",
									Query: &core.Query{
										Filters: []string{
											"{id:{_eq :{ $user_id}}}",
										},
									},
									Insert: &core.Insert{
										Filters: []string{
											"{id:{_eq :{ $user_id}}}",
										},
									},
								},
							}},
					},
		*/}
	//	config.AddRoleTable("user", "users", core.Query{})
	gj, err := core.NewGraphJin(config, db)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/v1/graphql", func(w http.ResponseWriter, r *http.Request) {

		payload, _ := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading body: %v", err)
			http.Error(w, "can't read body", http.StatusBadRequest)
			return
		}

		var body Request
		json.Unmarshal(payload, &body)

		res, err := gj.GraphQL(context.Background(), body.Query, body.Variables, nil)
		if err != nil {
			log.Printf("Error processing request: %v", err)
			http.Error(w, "can't process request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	})

	http.ListenAndServe(":8000", nil)
}

func filterDestructive(source []schema.Change) []schema.Change {
	var keep []schema.Change
	for _, change := range source {
		if destructive(change) {
			continue
		}
		switch c := change.(type) {
		case *schema.ModifySchema:
			c.Changes = filterDestructive(c.Changes)
		case *schema.ModifyTable:
			c.Changes = filterDestructive(c.Changes)
		}
		keep = append(keep, change)
	}
	return keep
}

func destructive(change schema.Change) bool {
	switch change.(type) {
	case *schema.DropSchema, *schema.DropTable, *schema.DropIndex, *schema.DropCheck,
		*schema.DropAttr, *schema.DropForeignKey, *schema.DropColumn:
		return true
	}
	return false
}
