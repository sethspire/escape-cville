# create the table once, manually
aws dynamodb create-table \
  --table-name escape-cville-table \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions \
      AttributeName=route,AttributeType=S \
      AttributeName=timestamp,AttributeType=S \
  --key-schema \
      AttributeName=route,KeyType=HASH \
      AttributeName=timestamp,KeyType=RANGE \
  --region us-east-1