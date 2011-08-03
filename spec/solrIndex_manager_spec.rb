require "rspec"
require "pp"
require_relative '../lib/solr_index_manager'

describe SolrIndexManager do
  before(:each) do
    Readline.stub(:readline).and_return 'y'
    module Kernel
      def `(cmd)
        random = Random.new(10)
        itr = (1..20)
        if cmd.start_with? 'hadoop fs -du'
          return itr.map { |n|
            r = random.rand(10..50)
            "#{r*1024*1024*1024} solrindex/test_20110730/part-r-#{n}"
          }.join "\n"
        elsif cmd.start_with? 'hadoop fs -cat'
          return "key '#{"%02d" % /-(\d+)/.match(cmd)[1].to_s.to_i}': 62700332 documents"
        elsif cmd.start_with? 'ls /'
          return itr.map { |n| "#{n}" }.join("\n")
        elsif cmd.start_with? 'du -k'
          return "20110104/t2342"
        else
          #puts cmd
        end
      end
    end
    #args = [
    #    'solrindex/test_20110730',
    #    '/merge_to/test_20110730',
    #    '/copy_to/test_20110730',
    #    '/move_to/test_20110730',
    #]
    @args = [
        {
            key_filter: '',
            hadoop_src: 'solrindex/test_20110730',
            copy_dst: '/copy_to/test_20110730',
            merge_dst: '/merge_to/test_20110730',
            move_dst: '/move_to/test_20110730/data/index',
            #            job_id: 'job_201107280750_0094',
            max_merge_size: '100Gb',
            dst_distribution:
                ['/data/a/solr/news/#{key}',
                 '/data/b/solr/news/#{key}',
                 '/data/c/solr/news/#{key}',
                 '/data/d/solr/news/#{key}',
                 '/data/e/solr/news/#{key}',
                 '/data/f/solr/news/#{key}']
        }
    ]

    #@manager = SolrIndexManager.new(args)
  end


  #it "testing1" do
  #  @manager.go()
  #end

  it "should do all without crashing" do
    @manager = SolrIndexManager.new(@args)
    @manager.go()
  end

  it "should yaml file" do
    opts = YAML::load(File.open("template.yaml"))
    #p opts
    manager = SolrIndexManager.new(opts)
    manager.go()
  end

  it 'should parse content from hadoop fs -ls' do
    list = @manager.get_files_with_info_from_hdfs('solrindex/test_20110730')
    random = Random.new(10)
    (1..20).each do |n|
      r = random.rand(10..50)
      list[n-1].to_s.should ==
          [
              "solrindex/test_20110730/part-r-#{n}",
              r*1024,
              "key '#{"%02d" % n.to_i}': 62700332 documents"
          ].to_s
    end
  end
end