defmodule TLake.Job.RawSnapshotTest do
  use ExUnit.Case
  require Explorer.DataFrame, as: DF

  setup_all do
    Application.stop(:t_lake)
  end

  def aws_config() do
    %{
      bucket: "fake_bucket",
      region: "fake_region",
      access_key_id: "fake_access_key_id",
      secret_access_key: "fake_secret_access_key"
    }
  end

  def server_id(), do: "ttq.x2.arabics.travian.com_20240612"

  def server_map() do
    %{
      name: "TTQ Arabics",
      version: "WW",
      speed: 2,
      url: "https://ttq.x2.arabics.travian.com",
      number_of_tribes: 5,
      start_date: "2024-06-12",
      timezone: "UTC",
      timezone_offset: 1,
      artifacts_date: "2024-07-27",
      building_plans_date: "2024-09-10",
      end_date: :wonder
    }
  end

  @tag :tmp_dir
  test "Run raw_snapshot", %{tmp_dir: root_path} do
    target_date = ~D[2024-05-29]
    f_filename = TLake.Job.Utils.f_filename(root_path, aws_config(), server_id())
    options = %{}

    assert :ok ==
             TLake.Job.RawSnapshot.run(
               f_filename,
               target_date,
               server_id(),
               server_map(),
               options
             )

    {:ok, local_fss} = f_filename.({TLake.Job.RawSnapshot.table_name(), target_date})
    assert File.exists?(local_fss.path)

    df = DF.from_parquet!(local_fss, lazy: false)
    assert DF.n_rows(df) > 0

    assert Enum.sort(DF.names(df)) ==
             Enum.sort(
               Enum.map(Map.keys(server_map()), fn atom -> :erlang.atom_to_binary(atom) end) ++
                 ["binary_record", "server_id", "target_date"]
             )
  end

  test "raw_snapshot__process__validated__input", %{test: test_name} do
    <<"test ", name::binary>> = :erlang.atom_to_binary(test_name)
    target_date = ~D[2024-05-29]

    resource_path = Path.join([Application.fetch_env!(:t_lake, :test_resources), name])
    input_path = Path.join([resource_path, "input.binary"])
    expected_output_path = Path.join([resource_path, "expected_output.parquet"])

    raw_snapshot = File.read!(input_path)
    df_expected_output = DF.from_parquet!(expected_output_path)

    df_outptut =
      TLake.Job.RawSnapshot.process(raw_snapshot, server_id(), target_date, server_map())

    # DF.to_parquet!(df_outptut, expected_output_path, compression: {:brotli, 11})

    f = fn df -> DF.sort_by(df, binary_record) |> DF.to_rows() end

    assert f.(df_outptut) == f.(df_expected_output)
  end
end
