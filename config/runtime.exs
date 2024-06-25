import Config

config :t_lake,
  aws_enable: System.get_env("TLAKE__AWS_BUCKET", "false"),
  aws_bucket: System.get_env("TLAKE__AWS_BUCKET"),
  aws_region: System.get_env("TLAKE__AWS_REGION"),
  aws_access_key_id: System.get_env("TLAKE__AWS_ACCESS_KEY_ID"),
  aws_secret_access_key: System.get_env("TLAKE__AWS_SECRET_ACCESS_KEY"),
  root_path: System.get_env("TLAKE__ROOT_PATH"),
  kaggle_bin: System.get_env("TLAKE__KAGGLE_BIN", "kaggle"),
  max_demand: System.get_env("TLAKE__MAX_DEMAND", "5") |> String.to_integer()
