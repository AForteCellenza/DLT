# Databricks notebook source

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "",
       "fs.azure.account.oauth2.client.secret": "",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9ffc6712-5ed6-4651-9b81-51f454114654/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://dlt@dltsa.dfs.core.windows.net/",
  mount_point = "/mnt/dlt",
  extra_configs = configs)

