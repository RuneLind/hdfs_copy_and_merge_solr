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

solr_version = "3.3.0"
solr_lib_path = "/usr/lib/solr/apache-solr-3.3.0/example/webapps/WEB-INF/lib/"

def get_job_status(job_id ="job_201106212134_0272")
  begin
    src = open("http://jobtracker.companybook.no:50030/jobdetails.jsp?jobid=#{job_id}").read()
    status = /<b>Status:\s*<\/b>\s*(.*)</.match(src).to_a[1]
    running_for = /<b>Running for:\s*<\/b>\s*(.*)</.match(src).to_a[1]
    complete = src.scan(/\d+.\d+\d+%/)
    return status, running_for, complete
  rescue Exception => ex
    return [ex.message]
  end
end

def get_part_to_date_from_hadoop(hadoop_src)
  printf "finding files and job.info on hdfs:"
  list_files_cmd = "hadoop fs -du #{hadoop_src} | grep part | gawk '{ if ($1>60)  print $0 }'"
  directory_list = %x[#{list_files_cmd}]
  total_size = 0
  total_num_docs = 0
  list = []
  directory_list.split("\n").each do |size_filename|
    size, filename = size_filename.split(/\s+/)
    job_info = %x[hadoop fs -cat #{filename.strip}/.job.info]
    print "."
    list << [filename, size, job_info]
    total_size += size.to_i
    total_num_docs += /\d+\s*documents/.match(job_info).to_s.to_i
  end
  puts " Total size:%6.2fGb DocCount:%9d" % [total_size/(1024*1024*1024.0), total_num_docs]
  return total_size, list
end

def sys_cmd(cmd, size=0, status="")
  size = size.to_i
  start = Time.now
  %x[#{cmd}] if !@test
  sleep 1
  time_used = Time.now - start
  out = "%s - [%04.1fGb %3.0fs] %s" % [cmd, size/(1024*1024*1024.0), time_used, status]
  out << " %5.1fMb/s" % [(size/time_used)/(1024*1024)] if size > 0
  puts out
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
  total_size, list = get_part_to_date_from_hadoop(@hadoop_src)
  done_size = 0
  cnt = 0
  list.each do |file, size, json|
    key = /\d+/.match(json).to_s
    path = "#{@local_src}/#{key}"
    done_size += size.to_i
    percentage = (done_size * 100) / total_size

    out = " %02d/%02d-%03d" % [cnt+=1, list.size, percentage] << "% "

    sys_cmd("hadoop fs -copyToLocal #{file} #{path}", size, out)

  end
end

if merge_index
  sys_cmd "rm -f solr_merge.out"
  merge = "java -cp #{solr_lib_path}/lucene-core-#{solr_version}.jar:#{solr_lib_path}/lucene-misc-#{solr_version}.jar:#{solr_lib_path}/lucene-analyzers-common-#{solr_version}.jar org/apache/lucene/misc/IndexMergeTool "
  makedir(@merge_dst)
  merge << @merge_dst + " "
  folders = get_folders(@local_src)
  puts "will merge [#{folders}] to:#{@merge_dst}"
  merge << folders.join(' ')
  merge << " >solr_merge.out"
  puts ""
  sys_cmd(merge)
end
