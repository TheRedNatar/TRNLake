defmodule TLake.Job.Utils do
  alias Explorer.DataFrame
  alias Explorer.Series
  require Explorer.DataFrame, as: DF

  @spec gen_server_id(server_url :: String.t(), server_starting_date :: Date.t()) :: binary()
  def gen_server_id(<<"https://", server_url::binary>>, server_starting_date) do
    "#{server_url}_#{Date.to_iso8601(server_starting_date, :basic)}"
  end

  @spec partitioned_path(root_path :: String.t(), attrs :: [{atom(), String.t()}, ...]) ::
          {:ok, String.t()} | {:error, File.posix()}
  def partitioned_path(root_path, attrs) do
    partitions = for {att, value} <- attrs, do: "#{att}=#{value}"
    path = Enum.join([root_path] ++ partitions, "/")

    case File.mkdir_p(path) do
      :ok -> {:ok, path}
      error -> error
    end
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
          server_id :: String.t(),
          target_date :: Date.t()
        ) ::
          (String.t() -> {:ok, FSS.entry()} | {:error, atom()})
  def f_filename(root_path, aws_config, server_id, target_date) do
    partitioned_path = Path.join(["server_id=#{server_id}", "target_date=#{target_date}"])

    fn
      {:aws, table_name} ->
        endpoint =
          "s3://#{aws_config[:bucket]}/#{String.replace(partitioned_path, "=", "___")}/#{table_name}.parquet"

        FSS.S3.parse(endpoint, config: Map.to_list(aws_config))

      table_name ->
        dir_path = Path.join([root_path, partitioned_path])
        path = Path.join([dir_path, "#{table_name}.parquet"])

        case File.mkdir_p(dir_path) do
          :ok -> {:ok, FSS.Local.from_path(path)}
          error -> error
        end
    end
  end
end
