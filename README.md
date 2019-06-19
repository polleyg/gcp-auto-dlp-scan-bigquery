# gcp-auto-dlp-scan-bigquery
An application that monitors new tables added to BigQuery and triggers a DLP when a new table is detected

```
protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.destinationTable:* OR
protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.tableCopy.destinationTable:* OR
protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable:* OR
protoPayload.methodName=tableservice.insert 
AND severity!=ERROR 
AND NOT protoPayload.authenticationInfo.principalEmail:@dlp-api.iam.gserviceaccount.com
```
