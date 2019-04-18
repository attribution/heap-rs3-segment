require 'heap-rs3-segment/loader'

module HeapRS3Segment
  MANIFEST_REGEXP = /\/sync_\d+\.json$/
  MANIFEST_BUCKET_PREFIX = 'manifests/sync_'
end