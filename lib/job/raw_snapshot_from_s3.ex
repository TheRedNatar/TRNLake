defmodule TLake.Job.RawSnapshotFromS3 do
  require Explorer.DataFrame, as: DF

  @moduledoc false

  def run(_f_filename, _target_date, _server_id, %{aws_enable: false}) do
    :ok
  end

  def run(f_filename, target_date, _server_id, _opts) do
    with(
      {:ok, local_fss} = f_filename.({TLake.Job.RawSnapshot.table_name(), target_date}),
      {:ok, aws_fss} = f_filename.({:aws, TLake.Job.RawSnapshot.table_name(), target_date})
    ) do
      case File.exists?(local_fss.path) do
        true -> :ok
        false -> download_valid_df(aws_fss, local_fss)
      end
    end
  end

  defp download_valid_df(aws_fss, local_fss) do
    with(
      {:ok, df} <- DF.from_parquet(aws_fss, lazy: true),
      true <- TLake.Job.Utils.is_df_valid?(df)
    ) do
      DF.to_parquet(df, local_fss, compression: {:brotli, 11})
    end
  end
end
