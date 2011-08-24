require 'readline'
require 'open-uri'

class SolrIndexManager
  SOLR_VERSION = "3.3.0"
  SOLR_LIB_PATH = "/usr/lib/solr/apache-solr-3.3.0/example/webapps/WEB-INF/lib/"

  def initialize(args)
    if args.size == 1
      @opts = args[0]
      @hadoop_src = @opts[:hadoop_src]
      @local_src = @opts[:copy_dst]
      @job_id = @opts[:job_id]
      @max_merge_size = @opts[:max_merge_size]
      @dst_distribution = @opts[:dst_distribution]
      @config_src_folder = @opts[:config_src_folder]
    else
      @hadoop_src = args[0]
      @local_src = args[1]
      @merge_dst = args[2]
      @move_dst = args[3]
      @job_id = args[4]
    end

    @copy_from_hadoop = !@hadoop_src.to_s.empty?
    @wait_for_job = !@job_id.to_s.empty?
  end

  #Batch = Struct.new(:batch_size, :folders)
  CopyInfo = Struct.new(:info, :folders, :merge_to, :hadoop_commands, :result_folder_name, :core_name)

  def go
    puts "Wait from job    :#{@job_id}" if @wait_for_job
    puts "Copy from hadoop :#{@hadoop_src}" if @copy_from_hadoop
    puts "Local path       :#{@local_src}"
    puts "Max merge size   :#{@max_merge_size}" if @max_merge_size
    puts "Config src_folder:#{@config_src_folder}" if @config_src_folder
    puts "dst_distribution :\n#{@dst_distribution.map { |d| " #{d}" }.join("\n")}" if @dst_distribution
    continue_or_not

    wait_for_job if @wait_for_job

    @max_merge_size = convert_from_gigabytes(@max_merge_size)

    if @copy_from_hadoop
      file_list = get_files_with_info_from_hdfs(@hadoop_src)
      batches = group_folders(file_list, @max_merge_size)
      copy_commands = create_copy_commands(batches)

      commands = create_commands(copy_commands)
      commands.each do |copy_info|
        puts "#{copy_info.info} will merge to:#{copy_info.merge_to + '/data/index'}"
        puts "will copy #{@config_src_folder} to '#{copy_info.merge_to}'" if @config_src_folder
        puts "will create core '#{copy_info.core_name}' with path:'#{copy_info.merge_to}' on '#{@opts[:core_admin]}'"
      end

      puts ""
      continue_or_not

      commands.each do |copy_info|
        puts copy_info.info
        copy_info.hadoop_commands.each do |hdfs_src, dest, size, status|
          sys_cmd("hadoop fs -copyToLocal #{hdfs_src} #{dest}", size, status)
        end

        merge_index(copy_info.folders, copy_info.merge_to + '/data/index')
        sys_cmd("cp -r #{@config_src_folder} #{copy_info.merge_to}/conf") if @config_src_folder

        rm_folders(copy_info.folders)

        #puts "will copy #{@opts[:config_src_folder]}"
        #puts "will create core '#{copy_info.core_name}' on '#{@opts[:core_admin]}'"
        #copy_info.hadoop_commands.each { |hdfs_src, dest| puts "#{hdfs_src} -> #{dest}" }

        #puts copyInfo.result_folder_name
      end
    end
  end

  def check_src_config_files()
    sys_cmd("ls #{@config_src_folder}")
  end

  def create_commands(copy_commands)
    cnt=0
    commands = []
    copy_commands.each do |info, parts|
      #puts info
      folders = []
      keys = []
      hadoop_commands = []
      parts.each { |hdfs_src, dest, size, key, status|
        #hadoop_commands << ["hadoop fs -copyToLocal #{hdfs_src} #{dest}", size, status]
        hadoop_commands << [hdfs_src, dest, size, key, status]
        folders << dest
        keys << key
      }

      key = "#{keys.first}-#{keys.last}"
      eval_data = @dst_distribution[(cnt+=1) % @dst_distribution.size-1]
      merge_to = eval('"' + eval_data + '"')# + '/data/index'

      core_name = @opts[:core_prefix].to_s + key
      #merge_index(folders, merge_to)
      #rm_folders(folders)
      #puts ""
      commands << CopyInfo.new(info, folders, merge_to, hadoop_commands, key, core_name)
    end
    commands
  end

  #def create_commands
  #  copy_commands.each do |info, parts|
  #    puts info
  #    folders = []
  #    keys = []
  #    parts.each { |hdfs_src, dest, size, key, status|
  #      sys_cmd("hadoop fs -copyToLocal #{hdfs_src} #{dest}", size, status)
  #      folders << dest
  #      keys << key
  #    }
  #
  #    key = "#{keys.first}-#{keys.last}"
  #    eval_data = @dst_distribution[(cnt+=1) % @dst_distribution.size-1]
  #    merge_to = eval('"' + eval_data + '"')
  #
  #    merge_index(folders, merge_to)
  #    rm_folders(folders)
  #    puts ""
  #    commands << {:info=>info, :folders=>folders, :merge_to=>merge_to }
  #  end
  #end


  def rm_folders(folders)
    folders.each do |folder|
      sys_cmd("rm -rf #{folder}")
    end
  end

  def create_copy_commands(batches)
    cnt = 0
    batches.map { |batch_size, folders|
      ["Batch [#{cnt+=1}/#{batches.size}] FolderCnt=#{folders.size} BatchSize:#{batch_size/1024}",
       create_copy_from_hadoop_commands(batch_size, folders)] }
  end

  FolderGroup = Struct.new(:total_size, :folder_info_list)

  def group_folders(folders, size_limit)
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

  def convert_from_gigabytes(merge_size_as_gb)
    merge_size = /\d+/.match(merge_size_as_gb).to_s.to_i * 1024
    return merge_size if merge_size > 0
    1000000000
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

  def continue_or_not
    puts "continue? (y/n)?"
    exit if Readline.readline != 'y'
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
    return list.sort_by { |name, size, job_info| job_info }
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

  def get_files(path)
    %x[ls #{path}].split("\n").map {|file| file }
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
      [file, path, size, key, out]
      #{:file=>file, :path=>path, :size=>size, :key=>key, :status=>out}
    end
  end

  def merge_index(folders, merge_dest)
    sys_cmd "rm -f solr_merge.out"
    merge = "java -cp #{SOLR_LIB_PATH}/lucene-core-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-misc-#{SOLR_VERSION}.jar:#{SOLR_LIB_PATH}/lucene-analyzers-common-#{SOLR_VERSION}.jar org/apache/lucene/misc/IndexMergeTool "
    makedir(merge_dest)
    merge << merge_dest + " "

    puts "will merge to: '#{merge_dest}'"
    merge << folders.join(' ')
    merge << " >solr_merge.out"
    sys_cmd(merge)
  end
end