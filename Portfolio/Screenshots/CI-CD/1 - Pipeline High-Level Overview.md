Inventory Batch process pipeline
	Inventory_process_pipeline (Run Business Systems) -> Operation Status
						utility_getmetadata_pipeline (Business Pipeline)
								utility_status_message_pipeline (Send Business Notification) switch to determine email 										message to call based on status of process; success, fail, undefined)
										utility_notification_pipeline (Handle User Notification - Email module)
												
									
		