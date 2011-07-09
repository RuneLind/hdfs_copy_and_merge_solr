require 'readline'
require 'open-uri'

if (ARGV.size < 3)
  puts "need arguments: hdfs_path copy_destination merged_destination"
  puts "hdfs_path:         (example:'solrindex/news_20110706') if files already on disk just let this arg be ''"
  puts "copy_destination:  (example:'/data/f/to_be_merged/news_20110706')"
  puts "merged_destination (example:'/data/e/solr/news/news_20110706') dont want to merge? let this be ''"
  puts "job_id: (example:job_201106212134_0273)"
  puts "test: (true - for just output)"
  exit
end

@hadoop_src = ARGV[0] || 'solrindex/newsFinancialSentimentsEngNor_20110706'
@local_src = ARGV[1] || '/data/f/to_be_merged/fin_sent_20110706'
@merge_dst = ARGV[2] || '/data/e/solr/sentiments/financial_news_20110706'
@job_id = ARGV[3]
@test = ARGV[4] || false

copy_from_hadoop = !@hadoop_src.to_s.empty?
merge_index = !@merge_dst.to_s.empty?
wait_for_job = !@job_id.to_s.empty?

puts "Wait from job   :#{@job_id}" if wait_for_job
puts "Copy from hadoop:#{@hadoop_src}" if copy_from_hadoop
puts "Local path      :#{@local_src}"
puts "Merge index to  :#{@merge_dst}" if merge_index
puts "test            :#{@test}" if @test
puts "continue? (y/n)?"
exit if Readline.readline != 'y'

solr_version = "4.0-2011-05-19_08-42-38"
solr_lib_path = "/usr/lib/solr/apache-solr-4.0-2011-05-19_08-42-38/example/webapps/WEB-INF/lib/"

def get_job_status(job_id ="job_201106212134_0272")
  begin
    src = open("http://jobtracker.companybook.no:50030/jobdetails.jsp?jobid=#{job_id}").read()
    status = /<b>Status:\s*<\/b>\s*(.*)</.match(src).to_a[1]
    running_for = /<b>Running for:\s*<\/b>\s*(.*)</.match(src).to_a[1]
    complete =  src.scan(/\d+.\d+\d+%/)
    return status, running_for, complete
  rescue Exception => ex
    return [ex.message]
  end
end

def get_part_to_date_from_hadoop(hadoop_src)
  printf "finding files and job.info on hdfs:"
  list_files_cmd = "hadoop fs -du #{hadoop_src} | grep part | gawk '{ if ($1>60)  print $0 }'"
  directory_list = %x[#{list_files_cmd}]
  list = []
  directory_list.split("\n").each do |size_filename|
    size, filename = size_filename.split(/\s+/)
    job_info = %x[hadoop fs -cat #{filename.strip}/.job.info]
    print "."
    list << [filename, size, job_info]
  end
  puts ""
  list
end

def sys_cmd(cmd, size=0)
  size = size.to_i
  start = Time.now
  %x[#{cmd}] if !@test
  sleep 1
  time_used = Time.now - start
  puts "%s - [%6.2fs]" % [cmd, time_used] if size == 0
  puts "%s - [%6.2fs] %6.2fMb/s" % [cmd, time_used, (size/time_used)/(1024*1024)] if size > 0
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

if wait_for_job
  status = []
  loop do
    status = get_job_status(@job_id)
    puts "job [#{@job_id}] [#{status[0]}] time [#{status[1]}] map/reduce:#{status[2]} #{Time.now}       "
    break if status[0] != 'Running'
    sleep(60)
  end
  if status[0] != 'Succeeded'
    puts "\njob [#{@job_id}] failed! exiting"
    exit
  end
end

if copy_from_hadoop
  list = get_part_to_date_from_hadoop(@hadoop_src)
  puts ""
  list.each do |file, size, json|
    key = /\d+/.match(json).to_s
    path = "#{@local_src}/#{key}"
    sys_cmd("hadoop fs -copyToLocal #{file} #{path}", size)
  end
end

if merge_index
  sys_cmd "rm -f solr_merge.out"
  merge = "java -cp #{solr_lib_path}/lucene-core-#{solr_version}.jar:#{solr_lib_path}/lucene-misc-#{solr_version}.jar:#{solr_lib_path}/lucene-analyzers-common-#{solr_version}.jar org/apache/lucene/misc/IndexMergeTool "
  makedir(@merge_dst)
  merge << @merge_dst + " "
  folders = get_folders(@local_src)
  puts "will merge [#{folders}] to:#{@merge_dst}"
  merge << folders .join(' ')
  merge << " >solr_merge.out"
  puts ""
  sys_cmd(merge)
end
