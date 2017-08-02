dynamodb tables:
* Animals (part:"Animal #", gsi:LocationAnimalIndex (part:"Location", sort:"SubLocation"))
* AnimalHolds (part:"Animal #", sort:"Hold #")
* AnimalPetIDs (part:"Animal #", sort:"Created Date/Time")
* AnimalMemos (part:"Animal #", sort:"Memo Create Date", lsi:AnimalMemoTypeIndex (sort:"Memo Type::Memo Subtype"))
* AnimalBehaviorTests (part:"Animal #", sort:"Created Date/Time", lsi:AnimalBehaviorTestCategoryTestIndex(sort:"Behavior Category::Behavior Test"))

ImportEmailCsvToDynamoDB:

* Triggered by Simple Email Service on mail receipt
* Reads provided S3 object containing email content
* Identifies an table based on email subject
* Extracts the attachment portion of the email (base64-encoded CSV)
* Converts from base64-encoded CSV to json (array of objects)
* Translates CSV field names to DynamoDB attribute names
  * Remove " " and "/"
  * "#" -> "Id"
  
* batch-upserts to table

PruneInactiveAnimalsFromDynamoDB

* Triggered by Scheduled Event (midnight)
* Scans Animals for Stage=Inactive, 
(intentionally no index for this - nightly scan perf less important than minutely upsert perf)