require 'readline'
require 'open-uri'

class Options
  def initialize(options_file)
    @opt = YAML::load(File.open(options_file))
#    opt = {
#        key_filter: '',
#        hadoop_src: 'solrindex/test_20110730',
#        copy_dst: '/copy_to/test_20110730',
#        merge_dst: '/merge_to/test_20110730',
#        move_dst: '/move_to/test_20110730/data/index',
#        job_id: 'job_201107280750_0094',
#        max_merge_size: '50Gb',
#        dst_distribution:
#            ['/data/a/solr/news/#{key}',
#             '/data/b/solr/news/#{key}',
#             '/data/c/solr/news/#{key}',
#             '/data/d/solr/news/#{key}',
#             '/data/e/solr/news/#{key}',
#             '/data/f/solr/news/#{key}']
#    }
#    save(opt)
  end

  def [](arg)
    @opt[arg]
  end

  def save(yaml)
    File.open("template.yaml", 'w:UTF-8') { |out| YAML::dump(yaml, out) }
  end
end

class SolrIndexManager
  SOLR_VERSION = "3.3.0"
  SOLR_LIB_PATH = "/usr/lib/solr/apache-solr-3.3.0/example/webapps/WEB-INF/lib/"

  def initialize(args)
    if args.size == 1
      @opts = args[0] #Options.new(args[0])
      @hadoop_src = @opts[:hadoop_src]
      @local_src = @opts[:copy_dst]
      @merge_dst = @opts[:merge_dst]
      @move_dst = @opts[:move_dst]
      @job_id = @opts[:job_id]
      @max_merge_size = @opts[:max_merge_size]
      @dst_distribution = @opts[:dst_distribution]
    else
      @hadoop_src = args[0]
      @local_src = args[1]
      @merge_dst = args[2]
      @move_dst = args[3]
      @job_id = args[4]
    end

    @copy_from_hadoop = !@hadoop_src.to_s.empty?
    @merge_index = !@merge_dst.to_s.empty?
    @move_index = !@move_dst.to_s.empty?
    @wait_for_job = !@job_id.to_s.empty?
  end

  def go
    puts "Wait from job    :#{@job_id}" if @wait_for_job
    puts "Copy from hadoop :#{@hadoop_src}" if @copy_from_hadoop
    puts "Local path       :#{@local_src}"
    puts "Merge index to   :#{@merge_dst}" if @merge_index
    puts "Move index to    :#{@move_dst}" if @move_index
    puts "Max merge size   :#{@max_merge_size}" if @max_merge_size
    puts "dst_distribution :#{@dst_distribution}" if @dst_distribution
    puts "continue? (y/n)?"
    exit if Readline.readline != 'y'

    @max_merge_size = /\d+/.match(@max_merge_size).to_s.to_i * 1024
    @max_merge_size = 1000000000 if @max_merge_size == 0

    wait_for_job if @wait_for_job

    total_size, file_list = get_files_with_info_from_hdfs(@hadoop_src)
    batches = crete_batches(file_list, @max_merge_size)

    sizes = batches.map { |batch_size, batch| "#{batch_size/1024}Gb" }.join(',')
    puts "Will merge in #{batches.size} batches with sizes=#{sizes}"

    if @copy_from_hadoop
      copy_comands = create_copy_commands(batches)
    end


    puts "continue? (y/n)?"
    exit if Readline.readline != 'y'
  end

  def create_copy_commands(batches)
    cnt = 0
    batches.each do |batch_size, folders|
      [ "Batch [#{cnt+=1}/#{batches.size}] FolderCnt=#{folders.size} BatchSize:#{batch_size/1024}",
      create_copy_from_hadoop_commands(batch_size, folders) ]
    end
    #copy_from_hadoop(total_size, batch) if @copy_from_hadoop

    #if @merge_index
    #  folders = get_folders(@local_src)
    #  merge_parts = folders #analyze(folders)
    #  merge_parts.each do |adjusted_folders|
    #    merge_index(adjusted_folders)
    #  end
    #end
    #move_index if @move_index
  end

  def crete_batches(folders, size_limit)
    list_of_batches = []
    batch = []
    total_size = 0
    folders.each do |name, size, job_info|
      if (total_size+size > size_limit && batch.size > 0)
        list_of_batches << [total_size, batch]
        total_size = 0
        batch = []
      end
      total_size += size
      batch << [name, size, job_info]
    end
    list_of_batches << [total_size, batch]
    list_of_batches
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

  def get_files_with_info_from_hdfs(hadoop_src)
    printf "finding files and job.info on hdfs:"
    list_files_cmd = "hadoop fs -du #{hadoop_src} | grep part | gawk '{ if ($1>60)  print $0 }'"
    directory_list = %x[#{list_files_cmd}]
    total_size = 0
    total_num_docs = 0
    list = []
    directory_list.split("\n").each do |size_filename|
      size, filename = size_filename.split(/\s+/)
      size = size.to_i / (1024*1024)
      job_info = %x[hadoop fs -cat #{filename.strip}/.job.info]
      print "."
      list << [filename, size, job_info]
      total_size += size
      total_num_docs += /\d+\s*documents/.match(job_info).to_s.to_i
    end
    puts " Total size:%6.2fGb DocCount:%9d" % [total_size/(1024.0), total_num_docs]
    return total_size, list.sort_by { |name, size, job_info| job_info }
  end

  def sys_cmd(cmd, size=0, status="")
    #size = size.to_i
    start = Time.now
    %x[#{cmd}]
    sleep(0.1)
    time_used = Time.now - start
    out = "#{cmd} - ["
    out << "%04.1fGb " % [size/(1024.0)] if size > 0
    out << "%3.0fs] %s" % [time_used, status]
    out << " %5.1fMb/s" % [(size/time_used)] if size > 0
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

  def create_copy_from_hadoop_commands(total_size, file_info_list)
    done_size = 0
    cnt = 0
    file_info_list.map do |file, size, json|
      key = /\d+/.match(json).to_s
      path = "#{@local_src}/#{key}"
      done_size += size.to_i
      percentage = (done_size * 100) / total_size

      out = " %02d/%02d-%03d" % [cnt+=1, file_info_list.size, percentage] << "% "
      ["hadoop fs -copyToLocal #{file} #{path}", size, out]
    end
  end

  def copy_from_hadoop(total_size, file_info_list)
    done_size = 0
    cnt = 0
    file_info_list.each do |file, size, json|
      key = /\d+/.match(json).to_s
      path = "#{@local_src}/#{key}"
      done_size += size.to_i
      percentage = (done_size * 100) / total_size

      out = " %02d/%02d-%03d" % [cnt+=1, file_info_list.size, percentage] << "% "
      sys_cmd("hadoop fs -copyToLocal #{file} #{path}", size, out)
    end
  end

  def merge_index(folders)
    sys_cmd "rm -f solr_merge.out"
    merge = "java -cp #{SOLR_LIB_PATH}/lucene-core-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-misc-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-analyzers-common-#{SOLR_VERSION}.jar org/apache/lucene/misc/IndexMergeTool "
    makedir(@merge_dst)
    merge << @merge_dst + " "

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