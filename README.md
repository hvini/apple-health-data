### Should enable following API's

1. Cloud Build
1. Cloud Run
1. Eventarc
1. Dataflow

### Create separate service account for terraform and dataflow

### Terraform service account should contain following roles
1. roles/bibgquery.admin
1. roles/cloudfunctions.admin
1. roles/run.admin
1. roles/storage.admin
1. roles/pubsub.admin
1. roles/iam.serviceAccountUser

### Dataflow service account should contain the roles/storage.admin role

### Add roles/pubsub.publisher role into storage service account

```
PROJECT_ID=$(gcloud config get-value project)
```

```
PROJECT_NUMBER=$(gcloud projects list --filter="project_id:$PROJECT_ID" --format='value(project_number)')
```

```
SERVICE_ACCOUNT=$(gsutil kms serviceaccount -p $PROJECT_NUMBER)
```

```
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/pubsub.publisher
```

### terraform plan & terraform apply