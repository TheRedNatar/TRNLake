defmodule TLake.Job.SnapshotToKaggle do
  defp metadata_template(server_name, server_start_date, target_date) do
    """
    {
        "title": "Snapshots -- #{server_name} -- #{server_start_date}",
        "subtitle": "Daily map.sql snapshot collected from Travian and cleaned by TheRedNatar",
        "description": "This datasets contains serveral snapshots (one per day) with evey village public information plus some server information like starting date or speed.",
        "id": "timoboz/my-awesome-dataset",
        "id_no": 12345,
        "licenses": [
            {
                "name": "CC0-1.0"
            }
        ],
        "resources": [
            ,
            {
                "path": "my-awesome-extra-file.txt",
                "description": "This is my awesome extra file!"
            }
        ],
        "keywords": [
            "beginner",
            "tutorial"
        ]
    }
    """
  end

  defp metadata_resources(path, resource_target_date) do
    """
    {
                "path": "#{path}",
                "description": "Snapshot collecte at #{resource_target_date}",
                "schema": {
                    "fields": [
                        {
                            "name": "server_id",
                            "description": "Server unique identifier, composed by server_url + (server) start_date.",
                            "type": "string"
                        },
                        {
                            "name": "target_date",
                            "description": "Snapshot collection date.",
                            "type": "datetime"
                        },
                        {
                            "name": "alliance_id",
                            "description": "Alliance unique identifier by server",
                            "type": "uuid"
                        },
    {
                            "name": "alliance_name",
                            "description": "Alliance name",
                            "type": "string"
                        }
                    ]
                }
            }
    """
  end
end
