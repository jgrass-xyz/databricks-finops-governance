# Security and configuration hygiene

This repository must not contain credentials or organization-specific identifiers.

- Keep workspace hosts, account/workspace IDs, user emails, service-principal IDs,
  Slack destinations, and customer names out of tracked files.
- Keep tokens and secret values in Databricks secret scopes or the deployment
  environment. Never place them in bundle variables or examples.
- Copy `config/excluded_clusters.example.py` to the Git-ignored
  `config/excluded_clusters.py` before adding real exclusions.
- Do not commit `.databricks/`, Terraform state, environment files, generated
  deployment metadata, or exported workspace configuration.
- Use `example.com`, `example-workspace.cloud.databricks.com`, and clearly synthetic
  IDs in tests and documentation.

Before publishing changes, run the local tests, bundle validation, a secret scanner,
and explicit searches for organization names, emails, hosts, workspace/account IDs,
Slack IDs, and absolute user paths.
