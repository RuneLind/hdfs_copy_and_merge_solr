if(ARGV.size < 3)
  puts "need arguments: hdfs_path copy_destination merged_destination"
  puts "hdfs_path:         (example:'solrindex/news_20110706') if files already on disk just let this arg be ''"
  puts "copy_destination:  (example:'/data/f/to_be_merged/news_20110706')"
  puts "merged_destination (example:'/data/e/solr/news/news_20110706') dont want to merge? let this be ''"
  puts "test: (true - for just output)"
  exit
end

@hadoop_src = ARGV[0] || 'solrindex/newsFinancialSentimentsEngNor_20110706'
@local_src = ARGV[1] || '/data/f/to_be_merged/fin_sent_20110706'
@merge_dst = ARGV[2] || '/data/e/solr/sentiments/financial_news_20110706'
@test = ARGV[3] || false

copy_from_hadoop = @hadoop_src != nil && @hadoop_src.size > 0 ? true : false
merge_index = @merge_dst != nil && @merge_dst.size > 0 ? true : false

solr_version = "4.0-2011-05-19_08-42-38"
solr_lib_path = "/usr/lib/solr/apache-solr-4.0-2011-05-19_08-42-38/example/webapps/WEB-INF/lib/"

def get_part_to_date_from_hadoop
  printf "finding files and job.info on hdfs:"
  list_files_cmd = "hadoop fs -du #{@hadoop_src} | grep part | gawk '{ if ($1>60)  print $2 }'"
  directory_list = %x[#{list_files_cmd}]
  map = {}
  directory_list.split("\n").each do |file|
    part = /part-.*/.match(file)
    job_info = %x[hadoop fs -cat #{file.strip}/.job.info]
    printf "."
    map[part.to_s]=job_info
  end
  puts ""
  map
end

def sys_cmd(cmd)
  puts cmd
  %x[#{cmd}] if !@test
end

def makedir(path)
  mkdir = 'mkdir -p ' + path
  sys_cmd(mkdir)
end

def get_folders(path)
  folders = []
  %x[ls #{path}].split("\n").each do |sub_folder|
    folder = "#{path}/#{sub_folder} "
    folders << folder
  end
  folders
end

if copy_from_hadoop
  map = get_part_to_date_from_hadoop
  puts ""
  map.each do |part, json|
    key = /\d+/.match(json).to_s
    path = "#{@local_src}/#{key}"
    sys_cmd "hadoop fs -copyToLocal #{@hadoop_src}/#{part} #{path}"
  end
end

if merge_index
  merge = "java -cp #{solr_lib_path}/lucene-core-#{solr_version}.jar:#{solr_lib_path}/lucene-misc-#{solr_version}.jar:#{solr_lib_path}/lucene-analyzers-common-#{solr_version}.jar org/apache/lucene/misc/IndexMergeTool "
  makedir(@merge_dst)
  merge << @merge_dst + " "
  folders = get_folders(@local_src)
  puts "will merge #{folders} to:#{@merge_dst}"
  merge << folders .join(' ')
  merge << " >solr_merge.out"
  puts ""
  sys_cmd(merge)
end

