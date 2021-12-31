version 1.0

import "vcfGLuerAndIndex.wdl" as sub

struct RangeOutput {
    String genomic_range
    File pvcf_gz
    File tbi
}

workflow vcfGLuerScatter {
    input {
        File vcf_manifest
        String output_name
        Array[String] genomic_ranges
        Int genomic_range_bin_size = 25
        String spark_worker_instance_type = "mem3_ssd3_x12"
    }

    scatter (grange in genomic_ranges) {
        call sub.vcfGLuerAndIndex as inner {
            input:
            vcf_manifest = vcf_manifest,
            output_name = output_name + "." + sub(grange, ":", "."),
            genomic_range_filter = grange,
            genomic_range_bin_size = genomic_range_bin_size,
            spark_worker_instance_type = spark_worker_instance_type
        }
        RangeOutput out = object {
            genomic_range: grange,
            pvcf_gz: inner.pvcf_gz,
            tbi: inner.tbi
        }
    }

    output {
        Array[RangeOutput] outputs = out
    }
}
