-- Databricks notebook source
-- Here is a pipeline setting definition. Please change the value with your own notebook path.
{
    "name": "demo_DLT_CDC",
    "storage": "/tmp/dlt_cdc",  --CHANGE ME
    "configuration": {
        "pipelines.applyChangesPreviewEnabled": "true"
    },
    "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "libraries": [
        {
            "notebook": {
                "path": "/Users/quentin.ambard@databricks.com/DLT_CDC/00-Retail_Data_CDC_Generator" --CHANGE ME
            }
        },
        {
            "notebook": {
                "path": "/Users/quentin.ambard@databricks.com/DLT_CDC/2-Retail_DLT_CDC_SQL"  --CHANGE ME
            }
        }
    ],
    "target": "quentin_dlt_cdc",  --CHANGE ME
    "continuous": false,
    "development": true
}
