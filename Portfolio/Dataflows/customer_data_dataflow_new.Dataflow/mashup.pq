[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customer_data_from_dataflow_cleaned_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "signup_date", DestinationColumnName = "signup_date"], [SourceColumnName = "email", DestinationColumnName = "email"], [SourceColumnName = "age", DestinationColumnName = "age"], [SourceColumnName = "income.2", DestinationColumnName = "income.2"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "phone_number", DestinationColumnName = "phone_number"], [SourceColumnName = "subscription_type", DestinationColumnName = "subscription_type"], [SourceColumnName = "last_login", DestinationColumnName = "last_login"], [SourceColumnName = "referral_code", DestinationColumnName = "referral_code"], [SourceColumnName = "signup_date - Copy", DestinationColumnName = "signup_date - Copy"], [SourceColumnName = "Age_group", DestinationColumnName = "Age_group"], [SourceColumnName = "Age Group Sort", DestinationColumnName = "Age Group Sort"], [SourceColumnName = "income_bin", DestinationColumnName = "income_bin"], [SourceColumnName = "valid_record", DestinationColumnName = "valid_record"], [SourceColumnName = "Error Reason", DestinationColumnName = "Error Reason"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared customer_data_from_dataflow_cleaned = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "customer_data", ItemKind = "Table"]}[Data],
  #"Duplicated column" = Table.DuplicateColumn(#"Navigation 3", "signup_date", "signup_date - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"signup_date", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"customer_id", "signup_date", "email", "age", "income", "country_code", "phone_number", "subscription_type", "last_login", "referral_code", "signup_date - Copy"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Reordered columns", "income", Splitter.SplitTextByDelimiter("$"), {"income.1", "income.2"}),
  #"Replaced value" = Table.ReplaceValue(#"Split column by delimiter", ".00", ".25", Replacer.ReplaceText, {"income.2"}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Replaced value", {{"income.2", type number}}, "en-GB"),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type with locale", {"income.1"}),
  #"Added custom" = Table.AddColumn(#"Removed columns", "Invalid Income", each let r = try [income.2] in if r[HasError] then true else false),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Invalid signup date", each let r = try [signup_date] in if r[HasError] then true else false),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"age", Int64.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 1", "Invalid age", each let r = try [age] in if r[HasError] then true else false),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom 2", "Invalid email", each if not Text.Contains([email], "@") then true else false),
  #"Added custom 3" = Table.AddColumn(#"Inserted conditional column", "Invalid Last Login", each if [last_login] = null then true else if [last_login] = """ """ then true else false),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Invalid last login", each if Text.Trim([last_login]) = "" then true else if [last_login] = "malformed_timestamp" then true else false),
  #"Removed columns 1" = Table.RemoveColumns(#"Added custom 4", {"Invalid Last Login"}),
  #"Added custom 5" = Table.AddColumn(#"Removed columns 1", "Age Bin", each if [age] < 18 then "Under 18" else if [age] <= 30 then "18–30" else if [age] <= 40 then "31–40" else if [age] <= 50 then "41-50" else if [age] <= 60 then "51-60" else if [age] <= 70 then "61-70" else if [age] <= 80 then "71-80" else "Over 80"),
  #"Age group sort" = Table.AddColumn(#"Added custom 5", "Age Group Sort", each if [age] < 18 then 1 else if [age] <= 30 then 2 else if [age] <= 40 then 3 else if [age] <= 50 then 4 else if [age] <= 60 then 5 else if [age] <= 70 then 6 else if [age] <= 80 then 7 else 8),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Age group sort", {{"Age Group Sort", type text}}),
  #"Added custom 6" = Table.AddColumn(#"Changed column type 2", "Income Bin", each if [income.2] < 20000 then "<20k" else if [income.2] < 50000 then "20k–50k" else if [income.2] < 100000 then "50k–100k" else ">100k"),
  #"Renamed columns" = Table.RenameColumns(#"Added custom 6", {{"Income Bin", "income_bin"}, {"Age Bin", "Age_group"}}),
  #"Inserted conditional column 1" = Table.AddColumn(#"Renamed columns", "valid_records", each if [Invalid Income] = false then true else if [Invalid age] = false then true else false),
  #"Added custom 7" = Table.AddColumn(#"Inserted conditional column 1", "valid_record", each if [Invalid Income] = true or [Invalid age] = true or [Invalid signup date] = true or [Invalid email]= true then false else true),
  #"Added custom 8" = Table.AddColumn(#"Added custom 7", "Error Reason", each if [Invalid Income] = true then "invalid income" else if [Invalid age] = true then "invalid age" else if [Invalid signup date] = true then "invalid signup date" else if [Invalid email] = true then "invalid email" else "valid record"),
  #"Removed columns 2" = Table.RemoveColumns(#"Added custom 8", {"valid_records"}),
  #"Removed columns 3" = Table.RemoveColumns(#"Removed columns 2", {"Invalid Income", "Invalid signup date", "Invalid age", "Invalid email", "Invalid last login"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Removed columns 3", {{"valid_record", type logical}, {"Error Reason", type text}, {"income_bin", type text}, {"Age_group", type text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type 3"),
  #"Filtered rows" = Table.SelectRows(#"Removed errors", each [signup_date] <= LastRefreshDate),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each [valid_record] = validRecordParam)
in
  #"Filtered rows 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customer_data_from_dataflow_invalid_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "signup_date", DestinationColumnName = "signup_date"], [SourceColumnName = "email", DestinationColumnName = "email"], [SourceColumnName = "age", DestinationColumnName = "age"], [SourceColumnName = "income.2", DestinationColumnName = "income.2"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "phone_number", DestinationColumnName = "phone_number"], [SourceColumnName = "subscription_type", DestinationColumnName = "subscription_type"], [SourceColumnName = "last_login", DestinationColumnName = "last_login"], [SourceColumnName = "referral_code", DestinationColumnName = "referral_code"], [SourceColumnName = "signup_date - Copy", DestinationColumnName = "signup_date - Copy"], [SourceColumnName = "Invalid Income", DestinationColumnName = "Invalid Income"], [SourceColumnName = "Invalid signup date", DestinationColumnName = "Invalid signup date"], [SourceColumnName = "Invalid age", DestinationColumnName = "Invalid age"], [SourceColumnName = "Invalid email", DestinationColumnName = "Invalid email"], [SourceColumnName = "Invalid last login", DestinationColumnName = "Invalid last login"], [SourceColumnName = "Age_group", DestinationColumnName = "Age_group"], [SourceColumnName = "Age Group Sort", DestinationColumnName = "Age Group Sort"], [SourceColumnName = "income_bin", DestinationColumnName = "income_bin"], [SourceColumnName = "valid_record", DestinationColumnName = "valid_record"], [SourceColumnName = "Error Reason", DestinationColumnName = "Error Reason"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared customer_data_from_dataflow_invalid = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "customer_data", ItemKind = "Table"]}[Data],
  #"Duplicated column" = Table.DuplicateColumn(#"Navigation 3", "signup_date", "signup_date - Copy"),
  #"Changed column type" = Table.TransformColumnTypes(#"Duplicated column", {{"signup_date", type date}}),
  #"Reordered columns" = Table.ReorderColumns(#"Changed column type", {"customer_id", "signup_date", "email", "age", "income", "country_code", "phone_number", "subscription_type", "last_login", "referral_code", "signup_date - Copy"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Reordered columns", "income", Splitter.SplitTextByDelimiter("$"), {"income.1", "income.2"}),
  #"Replaced value" = Table.ReplaceValue(#"Split column by delimiter", ".00", ".25", Replacer.ReplaceText, {"income.2"}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Replaced value", {{"income.2", type number}}, "en-GB"),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type with locale", {"income.1"}),
  #"Added custom" = Table.AddColumn(#"Removed columns", "Invalid Income", each let r = try [income.2] in if r[HasError] then true else false),
  #"Added custom 1" = Table.AddColumn(#"Added custom", "Invalid signup date", each let r = try [signup_date] in if r[HasError] then true else false),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom 1", {{"age", Int64.Type}}),
  #"Added custom 2" = Table.AddColumn(#"Changed column type 1", "Invalid age", each let r = try [age] in if r[HasError] then true else false),
  #"Inserted conditional column" = Table.AddColumn(#"Added custom 2", "Invalid email", each if not Text.Contains([email], "@") then true else false),
  #"Added custom 3" = Table.AddColumn(#"Inserted conditional column", "Invalid Last Login", each if [last_login] = null then true else if [last_login] = """ """ then true else false),
  #"Added custom 4" = Table.AddColumn(#"Added custom 3", "Invalid last login", each if Text.Trim([last_login]) = "" then true else if [last_login] = "malformed_timestamp" then true else false),
  #"Removed columns 1" = Table.RemoveColumns(#"Added custom 4", {"Invalid Last Login"}),
  #"Added custom 5" = Table.AddColumn(#"Removed columns 1", "Age Bin", each if [age] < 18 then "Under 18" else if [age] <= 30 then "18–30" else if [age] <= 40 then "31–40" else if [age] <= 50 then "41-50" else if [age] <= 60 then "51-60" else if [age] <= 70 then "61-70" else if [age] <= 80 then "71-80" else "Over 80"),
  #"Age group sort" = Table.AddColumn(#"Added custom 5", "Age Group Sort", each if [age] < 18 then 1 else if [age] <= 30 then 2 else if [age] <= 40 then 3 else if [age] <= 50 then 4 else if [age] <= 60 then 5 else if [age] <= 70 then 6 else if [age] <= 80 then 7 else 8),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Age group sort", {{"Age Group Sort", type text}}),
  #"Added custom 6" = Table.AddColumn(#"Changed column type 2", "Income Bin", each if [income.2] < 20000 then "<20k" else if [income.2] < 50000 then "20k–50k" else if [income.2] < 100000 then "50k–100k" else ">100k"),
  #"Renamed columns" = Table.RenameColumns(#"Added custom 6", {{"Income Bin", "income_bin"}, {"Age Bin", "Age_group"}}),
  #"Inserted conditional column 1" = Table.AddColumn(#"Renamed columns", "valid_records", each if [Invalid Income] = false then true else if [Invalid age] = false then true else false),
  #"Added custom 7" = Table.AddColumn(#"Inserted conditional column 1", "valid_record", each if [Invalid Income] = true or [Invalid age] = true or [Invalid signup date] = true or [Invalid email]= true then false else true),
  #"Added custom 8" = Table.AddColumn(#"Added custom 7", "Error Reason", each if [Invalid Income] = true then "invalid income" else if [Invalid age] = true then "invalid age" else if [Invalid signup date] = true then "invalid signup date" else if [Invalid email] = true then "invalid email" else "valid record"),
  #"Removed columns 2" = Table.RemoveColumns(#"Added custom 8", {"valid_records"}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Removed columns 2", {{"Error Reason", type text}, {"valid_record", type logical}, {"income_bin", type text}, {"Age_group", type text}, {"Invalid last login", type text}, {"Invalid email", type text}, {"Invalid Income", type text}, {"Invalid signup date", type text}, {"Invalid age", type text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type 3"),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [signup_date] <> LastRefreshDate),
  #"Filtered rows" = Table.SelectRows(#"Filtered rows 1", each [valid_record] <> validRecordParam)
in
  #"Filtered rows";
[Description = "Get the latest date for data"]
shared LastRefreshDate = #date(2025, 1, 15) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type date];
shared EnvironmentFlag = "DEV" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared customer_data_from_dataflow_cleaned_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "customer_data_cleaned_from_dataflow", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared validRecordParam = true meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type logical];
shared customer_data_from_dataflow_invalid_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "customer_data_from_dataflow_invalid", ItemKind = "Table"]}[Data]
in
  TableNavigation;
