// Lobbying Records
LOAD CSV FROM "file:///lob_lobbying.csv" AS row
WITH row LIMIT 10000
MERGE (lr:LobbyingRecord { id: row[0] })
ON CREATE SET 
    lr.registrant = row[2], 
    lr.isFirm = coalesce(row[3], FALSE),
    lr.client = row[5], 
    lr.amount = toFloat(coalesce(row[7], -1.0)), 
    lr.self = row[10], 
    lr.includeNSFS = coalesce(row[11], FALSE), 
    lr.use = coalesce(row[12], FALSE), 
    lr.ind = coalesce(row[13], FALSE),
    lr.year = toInteger(row[14]),
    lr.type = row[15],
    lr.typeLong = row[16],
    lr.affiliate = coalesce(row[17], FALSE)

CREATE CONSTRAINT FOR (lr:LobbyingRecord) REQUIRE lr.id IS UNIQUE;

// Firms
LOAD CSV WITH HEADERS FROM "file:///firms.csv" AS row
WITH row LIMIT 1000
MERGE (f:Firm {name: row.Client})
ON CREATE SET 
    f.catCode = row.Catcode,
    f.source = row.Source
CREATE CONSTRAINT FOR (f:Firm) REQUIRE f.name IS UNIQUE

// Subsidiary relationships
LOAD CSV WITH HEADERS FROM "file:///firm_parents.csv" AS row
WITH row LIMIT 1000
MATCH (child:Firm {name:row.Client})
MERGE (parent:Firm {name:row.Ultorg})
MERGE (child)-[s:SUBSIDIARY_OF]->(parent)

// Lobbying Record Client relationships
LOAD CSV WITH HEADERS FROM "file:///lob_lobbying.csv" AS row
WITH row LIMIT 10000
MATCH (lr:LobbyingRecord {id: row.Uniqid})
MATCH (f:Firm {name: row.Client})
MERGE (lr)-[:CLIENT]->(f)

// Categories
LOAD CSV WITH HEADERS FROM "file:///cat_codes.csv" AS row
CREATE (c:Category {name:row.Catname, catOrder: row.Catorder})
MERGE (i:Industry {name:row.Industry})
MERGE (s:Sector {name:row.Sector, nameLong:row.`Sector Long`})
MERGE (c)-[bi:BELONGS_TO]->(i)
MERGE (i)-[bs:BELONGS_TO]->(s)

// Firm Categories
LOAD CSV WITH HEADERS FROM "file:///cat_codes.csv" AS row
MATCH (cat:Category {name:row.Catname})
MATCH (f:Firm {catCode:row.Catcode})
MERGE (f)-[:PART_OF]->(cat)

// Lobbying Record - Firm relationships
:auto MATCH(lr:LobbyingRecord)
MATCH(f:Firm {name: lr.client})
CALL {
    WITH lr, f
    CREATE (lr)-[c:CLIENT]->(f)
    RETURN c
} IN TRANSACTIONS OF 1000 ROW
RETURN COUNT(c)

// Specific Issues
LOAD CSV WITH HEADERS FROM "file:///lob_issue.csv" as row
WITH row LIMIT 10000
MERGE (si:SpecificIssue {id:row.SI_ID})
ON CREATE SET
    si.specificIssue = row.SpecificIssue

// Issues (Generic)
LOAD CSV WITH HEADERS FROM "file:///lob_issue.csv" as row
WITH row LIMIT 10000
MERGE (i:Issue {id:row.IssueID})
ON CREATE SET
    i.issue = row.Issue

// Lobbying Record - Specific Issue relationships
LOAD CSV WITH HEADERS FROM "file:///lob_issue.csv" as row
WITH row LIMIT 10000
MATCH (lr:LobbyingRecord {id:row.Uniqid})
MATCH (si:SpecificIssue {id:row.SI_ID})
MERGE (lr)-[:LOBBIED_ON_ISSUE]->(si)

// Specific Issue - Issue relationships
LOAD CSV WITH HEADERS FROM "file:///lob_issue.csv" as row
WITH row LIMIT 10000
MATCH (i:Issue {id: row.IssueID})
MATCH (si:SpecificIssue {id: row.SI_ID})
MERGE (si)-[:MAIN_ISSUE]->(i)