import kotlin.text.StringBuilder

/**
 * Write pVCF header
 */
fun jointVcfHeader(cfg: JointConfig, aggHeader: AggVcfHeader, fieldsGen: JointFieldsGenerator): String {
    val ans = StringBuilder()
    ans.appendLine("##fileformat=VCFv4.3")

    // FILTER
    ans.appendLine("##FILTER=<ID=PASS,Description=\"All filters passed\">")

    // FORMAT/INFO
    ans.appendLine("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">")
    fieldsGen.fieldHeaderLines().forEach {
        ans.append("##")
        ans.appendLine(it)
    }

    // contig
    aggHeader.contigs.forEach {
        ans.appendLine("##" + aggHeader.headerLines.get(VcfHeaderLineKind.CONTIG to it)!!.lineText)
    }

    // column headers
    ans.append(
        listOf(
            "#CHROM",
            "POS",
            "ID",
            "REF",
            "ALT",
            "QUAL",
            "FILTER",
            "INFO",
            "FORMAT"
        ).joinToString("\t")
    )
    aggHeader.samples.forEach {
        ans.append("\t" + it)
    }
    ans.appendLine("")

    return ans.toString()
}
