require "rspec"
require_relative '../lib/solr_index_manager'

describe SolrIndexManager do
  it "testing" do
    Readline.stub(:readline).and_return 'y'
    module Kernel
      def `(cmd)
        itr = (1..8)
        if cmd.start_with? 'hadoop fs -du'
          return itr.map { |n| "#{n*10000000000} solrindex/test_index/part-r-#{n}" }.join "\n"
        elsif cmd.start_with? 'hadoop fs -cat'
          return "key '#{/\d+/.match(cmd).to_s.to_i}': 62700332 documents"
        elsif cmd.start_with? 'ls /merge_to/test'
          return itr.map {|n| "#{n}"}.join("\n")
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