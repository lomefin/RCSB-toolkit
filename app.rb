# frozen_string_literal: true

require 'open-uri'
require 'pathname'
require 'zlib'
require 'rexml/document'

class App
  def download_protein(p_name)
    puts "Downloading #{p_name}"
    url = "https://files.rcsb.org/download/#{p_name}.xml.gz"
    download(url, "/tmp/#{p_name}.xml.gz")
  end

  def download_and_unzip(uri, p_name)
    File.open("/tmp/#{p_name}.xml.gz", 'wb') do |file|
      file.write open(uri).read
    end
    puts 'Downloaded'
    puts 'Unzipping'
    Zlib::GzipReader.open("/tmp/#{p_name}.xml.gz") do |input_stream|
      File.open("#{Dir.pwd}/files/#{p_name}", 'w') do |output_stream|
        IO.copy_stream(input_stream, output_stream)
      end
    end
  end

  def get_protein(p_name)
    url = "https://files.rcsb.org/download/#{p_name}.xml.gz"
    puts "Downloading #{url}"
    download_and_unzip url, p_name
  end

  def extract_atom_sites(doc, protein_name)
    atom_sites = []
    doc.elements.each('//PDBx:atom_site') do |el|
      site = { atom_site_id: el['id'], protein_name: protein_name }
      el.elements.each do |cel|
        key = cel.name.downcase.to_sym
        val = cel.text
        site[key] = val
      end
      atom_sites << site
    end
    atom_sites
  end

  def atomsites_table_exists?(db)
    rows = db.execute <<-SQL
      SELECT name FROM sqlite_master WHERE type='table' AND name='atom_sites';
    SQL
    !rows.empty?
  end

  def build_atomsites_table(db)
    rows = db.execute <<-SQL
      create table atom_sites (
        atom_site_id varchar(30),
        protein_name varchar(30),
        b_iso_or_equiv varchar(40),
        cartn_x varchar(40),
        cartn_y varchar(40),
        cartn_z varchar(40),
        auth_asym_id varchar(40),
        auth_atom_id varchar(40),
        auth_comp_id varchar(40),
        auth_seq_id varchar(40),
        group_pdb varchar(40),
        label_alt_id varchar(40),
        label_asym_id varchar(40),
        label_atom_id varchar(40),
        label_comp_id varchar(40),
        label_entity_id varchar(40),
        label_seq_id varchar(40),
        occupancy varchar(40),
        pdbx_pdb_model_num varchar(40),
        type_symbol varchar(40),
        hidro_score numeric
      );
    SQL
  end

  def prepare_db
    require 'sqlite3'
    db = SQLite3::Database.new 'atomsites.db'
    build_atomsites_table(db) unless atomsites_table_exists?(db)
  end

  HIDRO_SCORES = { 'GLY' => -0.4, 'PRO' => -1.6, 'LEU' => 3.8, 'SER' => -0.8, 'VAL' => 4.2, 'ASP' => -3.5, 'ALA' => 1.8, 'PHE' => 2.8,
                   'LYS' => -3.9, 'ILE' => 4.5, 'GLU' => -3.5, 'TRP' => -0.9, 'ARG' => -4.5, 'ASN' => -3.5, 'THR' => -0.7, 'HIS' => -3.2,
                   'TYR' => -1.3, 'MET' => 1.9, 'GLN' => -3.5, 'CYS' => 2.5 }.freeze

  def save_atom_sites(sites)
    db = SQLite3::Database.new 'atomsites.db'
    sites.each do |site|
      cols = []
      vals = []
      upt_sql = <<-"SQL"
        SELECT atom_site_id, protein_name
        FROM atom_sites
        WHERE
          atom_site_id = '#{site[:atom_site_id]}'
          AND protein_name = '#{site[:protein_name]}'
      SQL
      next unless db.execute(upt_sql).empty?

      site[:hidro_score] = HIDRO_SCORES[site[:label_comp_id]]
      sleep(0.1)
      site.each do |key, val|
        cols << key
        vals << "'#{val}'"
      end

      cols_str = "(#{cols.join(',')})"
      sql = "INSERT INTO atom_sites #{cols_str} values (#{vals.join(',')})"
      puts sql
      db.execute(sql)
    end
  end

  def process_xml(protein_name)
    puts "Opening /files/#{protein_name}"
    protein_xml = File.open(Dir.pwd + "/files/#{protein_name}")
    puts 'Parsing...'
    doc = REXML::Document.new protein_xml
    sites = extract_atom_sites(doc, protein_name)
    puts 'Persising in DB...'
    save_atom_sites(sites)
  end

  def start
    prepare_db
    puts 'Structure name (ex: 5E2V): '
    protein_name = gets.chomp.downcase
    protein_name = '5e2v' if protein_name.empty?
    file_exists = Pathname.new(Dir.pwd + "/files/#{protein_name}").exist?
    # file_exists = path.exist? "files/#{protein_name}"
    get_protein(protein_name) unless file_exists

    process_xml(protein_name)
  end
end

App.new.start
