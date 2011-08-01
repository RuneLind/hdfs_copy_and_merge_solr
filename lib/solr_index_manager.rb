require 'readline'
require 'open-uri'

class SolrIndexManager
  SOLR_VERSION = "3.3.0"
  SOLR_LIB_PATH = "/usr/lib/solr/apache-solr-3.3.0/example/webapps/WEB-INF/lib/"

  def initialize(args)
    @hadoop_src = args[0]
    @local_src = args[1]
    @merge_dst = args[2]
    @move_dst = args[3]
    @job_id = args[4]
    @test = args[5] || false

    @copy_from_hadoop = !@hadoop_src.to_s.empty?
    @merge_index = !@merge_dst.to_s.empty?
    @move_index = !@move_dst.to_s.empty?
    @wait_for_job = !@job_id.to_s.empty?
  end

  def go
    puts "Wait from job   :#{@job_id}" if @wait_for_job
    puts "Copy from hadoop:#{@hadoop_src}" if @copy_from_hadoop
    puts "Local path      :#{@local_src}"
    puts "Merge index to  :#{@merge_dst}" if @merge_index
    puts "Move index to   :#{@move_dst}" if @move_index
    puts "test            :#{@test}" if @test
    puts "continue? (y/n)?"
    exit if Readline.readline != 'y'

    wait_for_job if @wait_for_job
    copy_from_hadoop if @copy_from_hadoop
    merge_index if @merge_index
    move_index if @move_index
  end

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
    out = "#{cmd} - ["
    out << "%04.1fGb " % [size/(1024*1024*1024.0)] if size > 0
    out << "%3.0fs] %s" % [time_used, status]
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

  def wait_for_job
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

  def copy_from_hadoop
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

  def merge_index
    sys_cmd "rm -f solr_merge.out"
    merge = "java -cp #{SOLR_LIB_PATH}/lucene-core-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-misc-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-analyzers-common-#{SOLR_VERSION}.jar org/apache/lucene/misc/IndexMergeTool "
    makedir(@merge_dst)
    merge << @merge_dst + " "
    folders = get_folders(@local_src)
    puts "will merge [#{folders}] to:#{@merge_dst}"
    merge << folders.join(' ')
    merge << " >solr_merge.out"
    puts ""
    sys_cmd(merge)
  end

  def move_index
    makedir(@move_dst)
    size = %x[du -k #{@merge_dst}].split(/\n/).last.to_i
    sys_cmd("mv #{@merge_dst} #{@move_dst}", size*1024)
  end
end