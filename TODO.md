# OpenSecrets Bulk Data Parsing TODO

### ToDo

- [ ] Set up as CLI tool with argparse
  - [ ] Create CSV and XLSX options for table output
  - [ ] Add database option for output? 
  - [ ] Create way of outputting to nodes/edges format
- [ ] Process y/null columns into boolean true/false

### In Progress

- [ ] Data Dictionary
  - [x] Add Lobby tables to data dictionary
  - [x] Add 527 tables to data dictionary
  - [x] Add PFD tables to data dictionary
  - [ ] Add Campaign Finance tables to data dictionary
    - [ ] Write modular way of reading in multiple folders (year)
  - [ ] Add Expend tables to data dictionary
    - [ ] Write modular way of reading in multiple folders (year)

### Complete

- [x] Read in basic text file as CSV w/ proper columns
- [x] Process lobbyist table w/ rows broken over multiple lines