[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dimdate_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "MonthName", DestinationColumnName = "MonthName"], [SourceColumnName = "Quarter", DestinationColumnName = "Quarter"], [SourceColumnName = "QuarterLabel", DestinationColumnName = "QuarterLabel"], [SourceColumnName = "WeekOfYear", DestinationColumnName = "WeekOfYear"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "DayOfWeek", DestinationColumnName = "DayOfWeek"], [SourceColumnName = "IsWeekend", DestinationColumnName = "IsWeekend"], [SourceColumnName = "FiscalYear", DestinationColumnName = "FiscalYear"], [SourceColumnName = "FiscalMonth", DestinationColumnName = "FiscalMonth"], [SourceColumnName = "FiscalQuarter", DestinationColumnName = "FiscalQuarter"], [SourceColumnName = "HolidayDate", DestinationColumnName = "HolidayDate"], [SourceColumnName = "IsUKHoliday", DestinationColumnName = "IsUKHoliday"], [SourceColumnName = "sk_date", DestinationColumnName = "sk_date"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dimdate = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dimdate", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 3", {{"Date", Order.Ascending}})
in
  #"Sorted rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "inventory_operations_data_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ProductID", DestinationColumnName = "ProductID"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "StockLevel", DestinationColumnName = "StockLevel"], [SourceColumnName = "ReorderPoint", DestinationColumnName = "ReorderPoint"], [SourceColumnName = "LastRestockDate", DestinationColumnName = "LastRestockDate"], [SourceColumnName = "Warehouse", DestinationColumnName = "Warehouse"], [SourceColumnName = "Region", DestinationColumnName = "Region"], [SourceColumnName = "Territory", DestinationColumnName = "Territory"], [SourceColumnName = "Supplier", DestinationColumnName = "Supplier"], [SourceColumnName = "LeadTimeDays", DestinationColumnName = "LeadTimeDays"], [SourceColumnName = "Latitude", DestinationColumnName = "Latitude"], [SourceColumnName = "Longitude", DestinationColumnName = "Longitude"], [SourceColumnName = "Country", DestinationColumnName = "Country"], [SourceColumnName = "Continent", DestinationColumnName = "Continent"], [SourceColumnName = "NeedsRestock", DestinationColumnName = "NeedsRestock"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared inventory_operations_data = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "inventory_operations_data", ItemKind = "Table"]}[Data],
  InvalidDate = Table.AddColumn(#"Navigation 3", "InvalidLastRestockDate", each let r = try [LastRestockDate] in if r[HasError] then true else false),
  #"unit cost fabricated" = Table.AddColumn(InvalidDate, "UnitCost_Fabricated", each if [Category] = "Electronics" then 50 else if [Category] = "Stationary" then 5 else if [Category] = "Hardware" then 15 else 20),
  #"units sold fabricated" = Table.TransformColumnTypes(Table.AddColumn(#"unit cost fabricated", "UnitSold_Fabricated", each if ([Category]) = "Electronics" then [StockLevel] * 0.6 else if [Category] = "Hardware" then [StockLevel] * 0.45 else if [Category] = "Office" then [StockLevel] * 0.7  else [StockLevel] * 0.4), {{"UnitSold_Fabricated", type number}}),
  #"Changed column type" = Table.TransformColumnTypes(#"units sold fabricated", {{"UnitCost_Fabricated", type number}, {"InvalidLastRestockDate", type logical}}),
  #"Added COGS" = Table.AddColumn(#"Changed column type", "cogs", each [UnitCost_Fabricated] * [UnitSold_Fabricated], type number),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added COGS", {{"cogs", type number}}),
  StockoutFlag = Table.AddColumn(#"Changed column type 1", "Stockout_Flag", each if [StockLevel] <= 0 then true else false),
  Overstock = Table.TransformColumnTypes(Table.AddColumn(StockoutFlag, "OverStockFlag", each if [StockLevel] > [ReorderPoint] * 2 then true else false), {{"OverStockFlag", type logical}}),
  OverstockThreshold = Table.TransformColumnTypes(Table.AddColumn(Overstock, "OverstockThreshold", each [ReorderPoint] * 2), {{"OverstockThreshold", type number}}),
  #"Physical Count" = Table.TransformColumnTypes(Table.AddColumn(OverstockThreshold, "Physical Count", each [StockLevel] + 
Number.RoundDown(Number.RandomBetween(-2,2) )), {{"Physical Count", Int64.Type}}),
  #"Stock Match Column" = Table.AddColumn(#"Physical Count", "StockMatchFlag", each if [Physical Count] = [StockLevel] then true else false),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Stock Match Column", {{"Stockout_Flag", type logical}, {"StockMatchFlag", type logical}}),
  IsOverstocked = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 2", "IsOverstocked", each if [StockLevel] > ([ReorderPoint] * 2.5) then true else false), {{"IsOverstocked", type logical}}),
  StockGap = Table.AddColumn(IsOverstocked, "StockGap ", each [ReorderPoint] - [StockLevel]),
  #"Restock Urgency" = Table.TransformColumnTypes(Table.AddColumn(StockGap, "RestockUrgency", each if [NeedsRestock] = "No" and [LeadTimeDays] > 7 then "High" else "Normal"), {{"RestockUrgency", type text}}),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Restock Urgency", {{"StockGap ", Int64.Type}}),
  // Difference between the System Stock Count and the Physical Stock count, to be able to create a rate percentage in efficiency in stock tracking
  #"Stock Match Difference" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type 3", "Stock Accuracy Difference", each 1.00 - ([StockLevel] - [Physical Count])/[StockLevel]), {{"Stock Accuracy Difference", type number}}),
  #"Stock Level Tracking" = Table.AddColumn(#"Stock Match Difference", "StockLevelTracking", each if [StockLevel] > [Physical Count] then "Overstock" else if [StockLevel] = [Physical Count] then "Balanced" else "Understock"),
  #"Changed column type 4" = Table.TransformColumnTypes(#"Stock Level Tracking", {{"StockLevelTracking", type text}})
in
  #"Changed column type 4";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Product_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sk_product", DestinationColumnName = "sk_product"], [SourceColumnName = "Category", DestinationColumnName = "Category"], [SourceColumnName = "ProductName", DestinationColumnName = "ProductName"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Dim_Product = let
  Source = inventory_operations_data,
  #"Grouped rows" = Table.Group(Source, {"Category", "ProductName"}, {{"AllData", each List.Count(List.Distinct([ProductName])), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Category", Order.Ascending}}),
  #"Added index" = Table.AddIndexColumn(#"Sorted rows", "Index", 0, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Category", "ProductName", "AllData"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns", {{"Index", "sk_product"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"AllData"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Location_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sk_location", DestinationColumnName = "sk_location"], [SourceColumnName = "Continent", DestinationColumnName = "Continent"], [SourceColumnName = "Country", DestinationColumnName = "Country"], [SourceColumnName = "Region", DestinationColumnName = "Region"], [SourceColumnName = "Warehouse", DestinationColumnName = "Warehouse"], [SourceColumnName = "Territory", DestinationColumnName = "Territory"], [SourceColumnName = "Latitude", DestinationColumnName = "Latitude"], [SourceColumnName = "Longitude", DestinationColumnName = "Longitude"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Dim_Location = let
  Source = inventory_operations_data,
  #"Removed other columns" = Table.SelectColumns(Source, {"Warehouse", "Region", "Territory", "Country", "Continent", "Latitude", "Longitude"}),
  #"Grouped rows" = Table.Group(#"Removed other columns", {"Warehouse", "Region", "Territory", "Country", "Continent", "Latitude", "Longitude"}, {{"Count", each List.Count(List.Distinct([Warehouse])), Int64.Type}, {"Count2", each List.Count(List.Distinct([Region])), Int64.Type}, {"Count3", each List.Count(List.Distinct([Territory])), Int64.Type}}),
  #"Sorted rows" = Table.Sort(#"Grouped rows", {{"Region", Order.Ascending}}),
  #"Reordered columns" = Table.ReorderColumns(#"Sorted rows", {"Continent", "Country", "Region", "Warehouse", "Territory", "Latitude", "Longitude", "Count", "Count2", "Count3"}),
  #"Added index" = Table.AddIndexColumn(#"Reordered columns", "Index", 1, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "sk_location"}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Renamed columns", {"sk_location", "Continent", "Country", "Region", "Warehouse", "Territory", "Latitude", "Longitude", "Count", "Count2", "Count3"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns 1", {"Count", "Count2", "Count3"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Supplier_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sk_supplier", DestinationColumnName = "sk_supplier"], [SourceColumnName = "Supplier", DestinationColumnName = "Supplier"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Dim_Supplier = let
  Source = inventory_operations_data,
  #"Removed other columns" = Table.SelectColumns(Source, {"Supplier"}),
  #"Grouped rows" = Table.Group(#"Removed other columns", {"Supplier"}, {{"Count", each List.Count(List.Distinct([Supplier])), Int64.Type}}),
  #"Added index" = Table.AddIndexColumn(#"Grouped rows", "Index", 1, 1, Int64.Type),
  #"Reordered columns" = Table.ReorderColumns(#"Added index", {"Index", "Supplier", "Count"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"Count"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"Index", "sk_supplier"}})
in
  #"Renamed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Status_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sk_status", DestinationColumnName = "sk_status"], [SourceColumnName = "NeedsRestock", DestinationColumnName = "NeedsRestock"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Dim_Status = let
  Source = inventory_operations_data,
  #"Removed other columns" = Table.SelectColumns(Source, {"NeedsRestock"}),
  #"Grouped rows" = Table.Group(#"Removed other columns", {"NeedsRestock"}, {{"Count", each List.Count(List.Distinct([NeedsRestock])), Int64.Type}}),
  #"Added index" = Table.AddIndexColumn(#"Grouped rows", "Index", 1, 1, Int64.Type),
  #"Renamed columns" = Table.RenameColumns(#"Added index", {{"Index", "sk_status"}}),
  #"Reordered columns" = Table.ReorderColumns(#"Renamed columns", {"sk_status", "NeedsRestock", "Count"}),
  #"Removed columns" = Table.RemoveColumns(#"Reordered columns", {"Count"})
in
  #"Removed columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Fact_Inventory_Process_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Fact_Inventory_Process = let
  Source = Table.NestedJoin(inventory_operations_data, {"ProductName", "Category"}, Dim_Product, {"ProductName", "Category"}, "Dim_Product", JoinKind.LeftOuter),
  #"Expanded Dim_Product" = Table.ExpandTableColumn(Source, "Dim_Product", {"sk_product"}, {"sk_product"}),
  #"Merged queries" = Table.NestedJoin(#"Expanded Dim_Product", {"Supplier"}, Dim_Supplier, {"Supplier"}, "Dim_Supplier", JoinKind.LeftOuter),
  #"Expanded Dim_Supplier" = Table.ExpandTableColumn(#"Merged queries", "Dim_Supplier", {"sk_supplier"}, {"sk_supplier"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded Dim_Supplier", {"NeedsRestock"}, Dim_Status, {"NeedsRestock"}, "Dim_Status", JoinKind.LeftOuter),
  #"Expanded Dim_Status" = Table.ExpandTableColumn(#"Merged queries 1", "Dim_Status", {"sk_status"}, {"sk_status"}),
  #"Merged queries 2" = Table.NestedJoin(#"Expanded Dim_Status", {"Supplier"}, Dim_Supplier, {"Supplier"}, "Dim_Supplier", JoinKind.LeftOuter),
  #"Expanded Dim_Supplier 1" = Table.ExpandTableColumn(#"Merged queries 2", "Dim_Supplier", {"sk_supplier"}, {"sk_supplier.1"}),
  #"Merged queries 3" = Table.NestedJoin(#"Expanded Dim_Supplier 1", {"Continent", "Country", "Region", "Warehouse", "Territory"}, Dim_Location, {"Continent", "Country", "Region", "Warehouse", "Territory"}, "Dim_Location", JoinKind.LeftOuter),
  #"Expanded Dim_Location" = Table.ExpandTableColumn(#"Merged queries 3", "Dim_Location", {"sk_location"}, {"sk_location"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded Dim_Location", {"ProductName", "Category", "Warehouse", "Region", "Territory", "Supplier", "Country", "Continent", "NeedsRestock"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"sk_product", "sk_supplier", "sk_status", "sk_supplier.1", "sk_location", "ProductID", "StockLevel", "ReorderPoint", "LastRestockDate", "LeadTimeDays", "Latitude", "Longitude", "InvalidLastRestockDate"}),
  #"Added custom" = Table.AddColumn(#"Reordered columns", "valid_recs", each if [InvalidLastRestockDate] = true then false else true),
  #"Merged queries 5" = Table.NestedJoin(#"Added custom", {"valid_recs"}, Dim_Valid, {"IsValid"}, "Dim_Valid", JoinKind.LeftOuter),
  #"Expanded Dim_Valid" = Table.ExpandTableColumn(#"Merged queries 5", "Dim_Valid", {"sk_valid"}, {"sk_valid"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Dim_Valid", {{"valid_recs", type logical}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Changed column type", {"valid_recs"}),
  #"Merged queries 4" = Table.NestedJoin(#"Removed columns 1", {"LastRestockDate"}, dimdate, {"Date"}, "dimdate", JoinKind.LeftOuter),
  #"Expanded dimdate" = Table.ExpandTableColumn(#"Merged queries 4", "dimdate", {"sk_date"}, {"sk_date"}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Expanded dimdate", {"sk_date", "sk_product", "sk_supplier", "sk_status", "sk_supplier.1", "sk_location", "ProductID", "StockLevel", "ReorderPoint", "LastRestockDate", "LeadTimeDays", "Latitude", "Longitude", "InvalidLastRestockDate", "sk_valid"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Reordered columns 1", {{"sk_valid", Int64.Type}, {"InvalidLastRestockDate", type logical}, {"Stockout_Flag", type logical}})
in
  #"Changed column type 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Valid_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "IsValid", DestinationColumnName = "IsValid"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Dim_Valid = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WMlDSUXJLzClOVYrViVYyBPJCikqBnFgA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [sk_valid = _t, IsValid = _t]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"IsValid", type logical}, {"sk_valid", Int64.Type}})
in
  #"Changed column type";
shared dimdate_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "dimdate", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared inventory_operations_data_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "inventory_operations_data", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Dim_Product_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Product", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Dim_Location_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Location", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Dim_Supplier_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Supplier", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Dim_Status_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Status", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Dim_Valid_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Valid", ItemKind = "Table"]}[Data]
in
  TableNavigation;
shared Fact_Inventory_Process_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  TableNavigation = Navigation_2{[Id = "Fact_Inventory_Process", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
