defmodule TLake.Job.RawSnapshotToS3 do
  alias Explorer.DataFrame
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  def run(f_filename, _target_date, _server_id, _opts) do
    with(
      {:ok, input_fss} <- f_filename.(TLake.Job.RawSnapshot.table_name()),
      {:ok, output_fss} <- f_filename.({:aws, TLake.Job.RawSnapshot.table_name()}),
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
