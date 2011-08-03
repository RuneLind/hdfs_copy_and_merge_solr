$LOAD_PATH << '.'
require 'lib/solr_index_manager'

args = [
    {
        key_filter: '',
        hadoop_src: 'solrindex/test_20110730',
        copy_dst: '/copy_to/test_20110730',
        merge_dst: '/merge_to/test_20110730',
        move_dst: '/move_to/test_20110730/data/index',
        #            job_id: 'job_201107280750_0094',
        max_merge_size: '150Gb',
        dst_distribution:
            ['/data/a/solr/news/#{key}',
             '/data/b/solr/news/#{key}',
             '/data/c/solr/news/#{key}',
             '/data/d/solr/news/#{key}',
             '/data/e/solr/news/#{key}',
             '/data/f/solr/news/#{key}']
    }
]

manager = SolrIndexManager.new(args)
manager.go()
