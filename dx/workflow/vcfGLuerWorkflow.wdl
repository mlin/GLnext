version 1.0

# build the vcfGLuer applet, then generate extern.wdl with:
#   java -jar dxCompiler.jar dxni -project project-xxxx -folder / --output extern.wdl -force
import "extern.wdl"

workflow vcfGLuerWorkflow {
    input {
        File vcf_manifest
        String output_name

        Array[String] genomic_ranges = []
        Boolean output_tbi = false

        Int genomic_range_bin_size = 25
        String spark_worker_instance_type = "mem3_ssd3_x12"
    }

    scatter (grange in (if (length(genomic_ranges) > 0) then genomic_ranges else [""])) {
        String grange_suffix = if (grange != "") then "." + sub(grange,":",".") else ""
        call extern.vcfGLuer {
            input:
            vcf_manifest = vcf_manifest,
            output_name = output_name + grange_suffix,
            genomic_range_filter = grange,
            genomic_range_bin_size = genomic_range_bin_size,
            worker_instance_type = spark_worker_instance_type
        }
    }

    if (output_tbi) {
        scatter(a_pvcf_gz in vcfGLuer.pvcf_gz) {
            call tabixIndex {
                input:
                bgz = a_pvcf_gz
            }
        }
    }

    output {
        Array[File] pvcf_gz = vcfGLuer.pvcf_gz
        Array[File]? tbi = tabixIndex.tbi
    }
}

task tabixIndex {
    input {
        File bgz
    }

    parameter_meta {
        bgz: "stream"
    }

    String bgzBasename = basename(bgz)

    command <<<
        ln -s '~{bgz}' '~{bgzBasename}'
        tabix '~{bgzBasename}'
    >>>

    runtime {
        docker: "ghcr.io/dnanexus-rnd/glnexus:v1.4.1"  # anything with tabix
        dx_instance_type: "mem1_ssd1_v2_x4"
    }

    output {
        File tbi = bgzBasename + ".tbi"
    }
}
