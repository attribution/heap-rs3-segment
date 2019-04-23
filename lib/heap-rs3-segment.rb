require 'heap-rs3-segment/logging'
require 'heap-rs3-segment/loader'
require 'heap-rs3-segment/processors/segment'

module HeapRS3Segment
  MANIFEST_REGEXP = /\/sync_\d+\.json$/
  MANIFEST_BUCKET_PREFIX = 'manifests/sync_'

  def self.logger
    HeapRS3Segment::Logging.logger
  end

  def self.logger=(log)
    HeapRS3Segment::Logging.logger = log
  end
end