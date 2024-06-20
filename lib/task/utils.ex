defmodule TLake.Job.Utils do
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  @moduledoc false

  @spec gen_server_id(server_url :: String.t(), server_starting_date :: Date.t()) :: binary()
  def gen_server_id(<<"https://", server_url::binary>>, server_starting_date) do
    "#{server_url}_#{Date.to_iso8601(server_starting_date, :basic)}"
  end

  def lit(value) do
    Series.from_list([value])
  end

  def lit(value, dtype) do
    Series.from_list([value], dtype: dtype)
  end

  @spec f_filename(
          root_path :: String.t(),
          aws_config :: %{
            bucket: String.t(),
            region: String.t(),
            access_key_id: String.t(),
            secret_access_key: String.t()
          },
          server_id :: String.t()
        ) ::
          ({String.t(), Date.t()} | {:aws, String.t(), Date.t()} ->
             {:ok, FSS.entry()} | {:error, atom() | Exception.t()})
  def f_filename(root_path, aws_config, server_id) do
    fn
      {:aws, table_name, target_date} ->
        endpoint =
          "s3://#{aws_config[:bucket]}/#{String.replace(partitioned_path(server_id, target_date), "=", "___")}/#{table_name}.parquet"

        FSS.S3.parse(endpoint, config: Map.to_list(aws_config))

      {table_name, target_date} ->
        dir_path = Path.join([root_path, partitioned_path(server_id, target_date)])
        path = Path.join([dir_path, "#{table_name}.parquet"])

        case File.mkdir_p(dir_path) do
          :ok -> {:ok, FSS.Local.from_path(path)}
          error -> error
        end
    end
  end

  defp partitioned_path(server_id, target_date) do
    Path.join(["server_id=#{server_id}", "target_date=#{target_date}"])
  end

  def read_parquets(f_filename, table_name, dates) do
    dates
    |> Enum.map(&read_parquet(f_filename, table_name, &1))
    |> Enum.filter(fn {atom, _} -> atom == :ok end)
    |> Enum.map(fn {_, df} -> df end)
  end

  defp read_parquet(f_filename, table_name, date) do
    case f_filename.({table_name, date}) do
      {:ok, filename} -> DF.from_parquet(filename, lazy: true)
      error -> error
    end
  end
end
