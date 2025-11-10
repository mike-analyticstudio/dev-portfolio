[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "customer_data_cleaned_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "customer_id", DestinationColumnName = "customer_id"], [SourceColumnName = "email", DestinationColumnName = "email"], [SourceColumnName = "signup_date_parsed", DestinationColumnName = "signup_date_parsed"], [SourceColumnName = "age_clean", DestinationColumnName = "age_clean"], [SourceColumnName = "income_clean", DestinationColumnName = "income_clean"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared customer_data_cleaned = let
  Source = Lakehouse.Contents(null),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "customer_data_cleaned", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared customer_data_cleaned_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "customer_data_cleaned", ItemKind = "Table"]}[Data]
in
  TableNavigation;
