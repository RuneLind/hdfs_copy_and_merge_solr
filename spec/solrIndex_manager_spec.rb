require "rspec"
require_relative '../lib/solr_index_manager'

describe SolrIndexManager do
  it "testing" do
    Readline.stub(:readline).and_return 'y'
    module Kernel
      def `(cmd)
        if cmd.start_with? 'hadoop fs -du'
          m = (1..4).map { |n| "#{n*10000000000}  solrindex/test_index/part-r-0000#{n}" }
          return m.join "\n"
        elsif cmd.start_with? 'hadoop fs -cat'
          return "key '201101': 62700332"
        elsif cmd.start_with? 'ls /merge_to/test'
          return "20110101\n20110102\n20110103\n20110104"
        elsif cmd.start_with? 'du -k'
          return "20110104/t2342"
        else
#          puts "[ #{cmd} ]"
        end
      end
    end

    args = [
        'solrindex/test_index',
        '/merge_to/test',
        '/copy_to/test',
        '/move_to/test',
    ]
    manager = SolrIndexManager.new(args)
    manager.stub(:sleep)
    manager.go()
  end
end