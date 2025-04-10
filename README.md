# Heap Reverse S3 processor

### Sample usage

```
args = {
  project_identifier: 'XXXXXX',
  s3_bucket: 'heap-rs3-atb-NNNN',
  revenue_fallback: 'revenue'
}

processor = Services::Segment::HeapProcessor.new(args[:project_identifier])
require 'heap-rs3-segment'

heap_rs3 = HeapRS3Segment::Loader.new(
  processor,
  args[:project_identifier],
  args[:s3_bucket],
  ENV['S3_DATABASE_EXPORT_ID'],
  ENV['S3_DATABASE_EXPORT_KEY']
)
heap_rs3.revenue_fallback << args[:revenue_fallback]
heap_rs3.skip_tables << 'global_view_any_page' << 'mg_taxonomy_page_viewed'
heap_rs3.process_single_sync = false

# heap_rs3.identify_only_users = true

heap_rs3.call
```


### Specific manifest with skip logic

```
args = {
  project_identifier: 'XXXXXX',
  s3_bucket: 'heap-rs3-atb-NNN',
  revenue_fallback: 'revenue'
}
processor = Services::Segment::HeapProcessor.new(args[:project_identifier])
require 'heap-rs3-segment'

heap_rs3 = HeapRS3Segment::Loader.new(
  processor,
  args[:project_identifier],
  args[:s3_bucket],
  ENV['S3_DATABASE_EXPORT_ID'],
  ENV['S3_DATABASE_EXPORT_KEY']
)
heap_rs3.revenue_fallback << args[:revenue_fallback]

manifest_sync = 'manifests/sync_1008461850.json'
list_opts = { bucket: heap_rs3.aws_s3_bucket, prefix: manifest_sync, delimiter: '/' }
resp = heap_rs3.s3.list_objects_v2(list_opts)
obj = resp.contents.first
heap_rs3.skip_before = Time.utc(2025, 1, 1)
heap_rs3.identify_only_users = true
heap_rs3.skip_types = [:track]
heap_rs3.skip_file = ->(file) do
  if match = file.match(/pageviews\/part-(\d+)/)
    # skip files with part number OUTSIDE of 10000..10999
    # e.g. only process files that match pageviews/part-10???
    unless (10000..10999).include?(match[1].to_i)
      return true
    end
  end
end
heap_rs3.process_sync(obj)
```