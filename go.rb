$LOAD_PATH << '.'
require 'lib/solr_index_manager'

args = [
    {
        :key_filter => '',
        :hadoop_src => 'solrindex/news20110820_all',
        :copy_dst => '/data/f/copy_to/news20110820_all',
        #:merge_dst => 'data/e/merge_to/test_20110730',
        #:move_dst => '/move_to/test_20110730/data/index',
        #            job_id: 'job_201107280750_0094',
        :max_merge_size => '150Gb',
        :dst_distribution =>
            ['/data/a/solr/news_20110821/#{key}/data/index',
             '/data/b/solr/news_20110821/#{key}/data/index',
             '/data/c/solr/news_20110821/#{key}/data/index',
             '/data/d/solr/news_20110821/#{key}/data/index',
             '/data/e/solr/news_20110821/#{key}/data/index',
             #'/data/f/solr/news_20110821/#{key}/data/index'
            ]
    }
]

manager = SolrIndexManager.new(args)
manager.go()
