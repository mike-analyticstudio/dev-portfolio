[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data]{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data];
[BindToDefaultDestination = true]
shared ecdc_web_source = let
  Source = Csv.Document(Web.Contents("https://opendata.ecdc.europa.eu/covid19/casedistribution/csv"), [Delimiter = ",", Columns = 12, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"dateRep", type date}, {"day", Int64.Type}, {"month", Int64.Type}, {"year", Int64.Type}, {"cases", Int64.Type}, {"deaths", Int64.Type}, {"countriesAndTerritories", type text}, {"geoId", type text}, {"countryterritoryCode", type text}, {"popData2019", Int64.Type}, {"continentExp", type text}, {"Cumulative_number_for_14_days_of_COVID-19_cases_per_100000", type number}})
in
  #"Changed column type";
