MATCH(f:Firm)-[c:CLIENT]-(lr:LobbyingRecord)
WHERE lr.year > 2000
RETURN f.name, sum(lr.amount)
ORDER BY sum(lr.amount) DESC
LIMIT 100