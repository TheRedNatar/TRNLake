defmodule TLake.Job.RawSnapshotToS3 do
  require Explorer.DataFrame, as: DF

  def run(f_filename, target_date, _server_id, _opts) do
    with(
      {:ok, input_fss} <- f_filename.({TLake.Job.RawSnapshot.table_name(), target_date}),
      {:ok, output_fss} <- f_filename.({:aws, TLake.Job.RawSnapshot.table_name(), target_date}),
      {:ok, df} <- DF.from_parquet(input_fss, lazy: true),
      :ok <-
        DF.to_parquet(df, output_fss,
          compression: {:brotli, 11},
          streaming: false
        )
    ) do
      :ok
    end
  end
end
