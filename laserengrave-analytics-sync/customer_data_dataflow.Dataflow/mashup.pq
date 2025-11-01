[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared customer_data = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "8af43c6b-4303-4b0b-8459-7621117771f2"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "customer_data", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
