package net.mlin.GLnext.data

import kotlin.test.Test

class VcfRecordTest {
    @Test
    fun detectsNonPassFilterValues() {
        check(!vcfRecordHasNonPassFilter("chr1\t1\t.\tA\tC\t.\tPASS\t.\tGT\t0/1"))
        check(!vcfRecordHasNonPassFilter("chr1\t1\t.\tA\tC\t.\t.\t.\tGT\t0/1"))
        check(vcfRecordHasNonPassFilter("chr1\t1\t.\tA\tC\t.\tLowQual\t.\tGT\t0/1"))
        check(vcfRecordHasNonPassFilter("chr1\t1\t.\tA\tC\t.\tLowQual;VQSRTranche\t.\tGT\t0/1"))
    }
}
