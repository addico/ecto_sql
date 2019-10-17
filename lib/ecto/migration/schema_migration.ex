defmodule Ecto.Migration.SchemaMigration do
  # Defines a schema that works with a table that tracks schema migrations.
  # The table name defaults to `schema_migrations`.
  @moduledoc false
  use Ecto.Schema

  import Ecto.Query, only: [from: 2]

  @primary_key false
  schema "schema_migrations" do
    field :version, :integer, primary_key: true
    timestamps updated_at: false
  end

  @opts [timeout: :infinity, log: false]

  def ensure_schema_migrations_table!(repo, opts) do
    table_name = repo |> get_source |> String.to_atom()
    table = %Ecto.Migration.Table{name: table_name, prefix: opts[:prefix]}
    meta = Ecto.Adapter.lookup_meta(repo.get_dynamic_repo())

    commands = [
      {:add, :version, :bigint, primary_key: true},
      {:add, :inserted_at, :naive_datetime, []}
    ]

    # DDL queries do not log, so we do not need to pass log: false here.
    repo.__adapter__.execute_ddl(meta, {:create_if_not_exists, table, commands}, @opts)
  end

  def versions(repo, prefix) do
    from(p in get_source(repo), select: type(p.version, :integer))
    |> Map.put(:prefix, prefix)
  end

  def up(repo, version, prefix) do
    %__MODULE__{version: version}
    |> Ecto.put_meta(prefix: prefix, source: get_source(repo))
    |> repo.insert(@opts)
  end

  # There are several forces at work that make this
  # operation more complicated than it should to be.
  #
  # - MySQL bug prevents DELETE from working when the
  #   database/table is fully qualified, (eg. using a prefix) and contains
  #   a WHERE clause. Calls to delete/2 work properly.
  #   https://bugs.mysql.com/bug.php?id=23413 (and several related duplicates)
  # - The most obvious workaround to the MySQL bug is to implement delete_all/2
  #   through a query followed by individual deletes. There are likely better
  #   workarounds.
  # - The delete_all/2 query was previously using schemaless syntax.
  #   Schemaless queries force us to select fields within the query and
  #   the result doesn't compose easily with delete/2.
  # - To remedy the above, we have to specify the schema module in our
  #   initial query so that structs are returned and easily deleted.
  # - delete/2 requires a primary key on the struct, which was added to
  #   the schema.
  # - delete_all/2 returns a tuple of the count of the items deleted and nil,
  #   at least with MySQL, so we'll imitate that result.
  def down(repo, version, prefix) do
    deleted =
      from(p in {get_source(repo), __MODULE__},
        where: p.version == type(^version, :integer))
        |> Map.put(:prefix, prefix)
        |> repo.all(@opts)
        |> Enum.map(&repo.delete!(&1, @opts))

    {Enum.count(deleted), nil}
  end

  def get_source(repo) do
    Keyword.get(repo.config, :migration_source, "schema_migrations")
  end
end
