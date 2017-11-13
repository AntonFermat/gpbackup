package utils_test

import (
	"bytes"

	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("utils/toc tests", func() {
	comment := utils.StatementWithType{ObjectType: "COMMENT", Statement: "-- This is a comment\n"}
	commentLen := uint64(len(comment.Statement))
	create := utils.StatementWithType{ObjectType: "DATABASE", Statement: "CREATE DATABASE somedatabase;\n"}
	createLen := uint64(len(create.Statement))
	role1 := utils.StatementWithType{ObjectType: "ROLE", Statement: "CREATE ROLE somerole1;\n"}
	role1Len := uint64(len(role1.Statement))
	role2 := utils.StatementWithType{ObjectType: "ROLE", Statement: "CREATE ROLE somerole2;\n"}
	role2Len := uint64(len(role2.Statement))
	BeforeEach(func() {
		toc, backupfile = testutils.InitializeTestTOC(buffer, "global")
	})
	Context("GetSqlStatementForObjectTypes", func() {
		It("returns statement for a single object type", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, backupfile)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", globalFile, "DATABASE")

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, backupfile)
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", commentLen+createLen, backupfile)
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", commentLen+createLen+role1Len, backupfile)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", globalFile, "DATABASE", "ROLE")

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			backupfile.ByteCount = commentLen + createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", commentLen, backupfile)

			globalFile := bytes.NewReader([]byte(comment.Statement + create.Statement))
			statements := toc.GetSQLStatementForObjectTypes("global", globalFile, "TABLE")

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("GetAllSqlStatements", func() {
		It("returns statement for a single object type", func() {
			backupfile.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", 0, backupfile)

			globalFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements("global", globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create}))
		})
		It("returns statement for a multiple object types", func() {
			backupfile.ByteCount = createLen
			toc.AddMetadataEntry("", "somedatabase", "DATABASE", 0, backupfile)
			backupfile.ByteCount += role1Len
			toc.AddMetadataEntry("", "somerole1", "ROLE", createLen, backupfile)
			backupfile.ByteCount += role2Len
			toc.AddMetadataEntry("", "somerole2", "ROLE", createLen+role1Len, backupfile)

			globalFile := bytes.NewReader([]byte(create.Statement + role1.Statement + role2.Statement))
			statements := toc.GetAllSQLStatements("global", globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{create, role1, role2}))
		})
		It("returns empty statement when no object types are found", func() {
			globalFile := bytes.NewReader([]byte(create.Statement))
			statements := toc.GetAllSQLStatements("global", globalFile)

			Expect(statements).To(Equal([]utils.StatementWithType{}))
		})
	})
	Context("SubstituteRedirectDatabaseInStatements", func() {
		wrongCreate := utils.StatementWithType{ObjectType: "TABLE", Statement: "CREATE DATABASE somedatabase;\n"}
		gucs := utils.StatementWithType{ObjectType: "DATABASE GUC", Statement: "ALTER DATABASE somedatabase SET fsync TO off;\n"}
		metadata := utils.StatementWithType{ObjectType: "DATABASE METADATA", Statement: "ALTER DATABASE somedatabase OWNER TO testrole;\n"}
		oldSpecial := utils.StatementWithType{ObjectType: "DATABASE", Statement: `CREATE DATABASE "db-special-chär$";
`}
		It("can substitute a database name in a CREATE DATABASE statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase;\n"))
		})
		It("can substitute a database name in an ALTER DATABASE OWNER statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{metadata}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase OWNER TO testrole;\n"))
		})
		It("can substitute a database name in a database GUC statement", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{gucs}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("ALTER DATABASE newdatabase SET fsync TO off;\n"))
		})
		It("doesn't modify a statement of the wrong type", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{wrongCreate}, "somedatabase", "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE somedatabase;\n"))
		})
		It("can substitute a database name if the old name contained special characters", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{oldSpecial}, `"db-special-chär$"`, "newdatabase")
			Expect(statements[0].Statement).To(Equal("CREATE DATABASE newdatabase;\n"))
		})
		It("can substitute a database name if the new name contained special characters", func() {
			statements := utils.SubstituteRedirectDatabaseInStatements([]utils.StatementWithType{create}, "somedatabase", `"db-special-chär$"`)
			Expect(statements[0].Statement).To(Equal(`CREATE DATABASE "db-special-chär$";
`))
		})
	})
})
