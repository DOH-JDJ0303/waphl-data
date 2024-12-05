#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

/*
=============================================================================================================================
    DEFINE FILE PATHS
=============================================================================================================================
*/
def meta_gba   = file(params.data).resolve('meta.gba.csv')
def meta_fastq = file(params.data).resolve('meta.fastq.csv')
def meta_fasta = file(params.data).resolve('meta.fasta.csv')
def gba        = file(params.data).resolve('gba.csv')
def gba_miss   = file(params.data).resolve('gba.miss.csv')


workflow {
    /*
    =============================================================================================================================
        GENERAL BACTERIAL ANALYSIS RESULTS (PHOENIX, THEIAPROK, RECAPP)
        - Download result files listed in 'meta.gba.csv'
        - Extract relevant fields and filter
    =============================================================================================================================
    */
    Channel.fromPath(meta_gba)
        .splitCsv(header: true, quote: '"')
        .unique()
        .filter{ it.id == 'null' && file(it.current).exists() }
        .map{ [ it.workflow, it.timestamp, file(it.current) ] }
        .splitCsv(header: true, elem: 2, sep: '\t')
        .map{ workflow, timestamp, results -> [workflow: workflow, timestamp: timestamp ] + formatResults(results) }
        .map{ [ it.id, [ timestamp: it.timestamp, workflow: it.workflow, species: it.species, qc: it.qc ] ] }
        .unique()
        .groupTuple(by: 0)
        .map{ id, data -> [ id: id ] + data.max{ it.timestamp } } // select most recent version of each sample
        .filter{ it.qc == 'FAIL' ? false : true } // select only samples that pass QC
        .set{ ch_gba }

    /*
    =============================================================================================================================
        FASTQ FILES
        - Pair FASTQ files with each sample from the general bacterial analysis
        - Only pairs original FASTQ files (i.e., those containing R1 and R2 in name)
    =============================================================================================================================
    */
    Channel.fromPath(meta_fastq)
        .splitCsv(header: true, quote: '"')
        .filter{ it.file ==~ /.*_R[12].*.fastq.gz/ }
        .map{ [ it.id_alt, [ it.file, it.current ] ] }
        .unique()
        .groupTuple(by: 0)
        .join( ch_gba.map{ [ it.id, it.species ] }, by: 0, remainder: true )
        .filter{ it[2] } // must contain species value
        .map{ id, fastqs, species -> [ id: id, species: species ] + selectFastqs( fastqs ) }
        .set{ ch_gba }

    /*
    ============================================================================================================================
        FASTA FILES
        - Pair an assembly file with each sample from the general bacterial analysis
        - PHoeNIx assemblies = "${id}.scaffolds.fa.gz", TheiaProk assemblies = "${id}_contigs.fasta"
    =============================================================================================================================
    */
    Channel.fromPath(meta_fasta)
        .splitCsv(header: true, quote: '"')
        .filter{ it.file == "${it.id}.scaffolds.fa.gz" || it.file == "${it.id}_contigs.fasta" }
        .map{ [ it.id_alt, [ it.current, it.timestamp ]  ] }
        .unique()
        .groupTuple(by: 0)
        .map{ id, data -> [ id, data.max{ it[1] }[0] ] }
        .join( ch_gba.map{ [ it.id, it.species, it.fastq_1, it.fastq_2 ] }, by: 0, remainder: true )
        .filter{ it[2] } // must contain species value
        .map{ id, assembly, species, fastq_1, fastq_2 -> [ sample: id, taxa: species, assembly: assembly, fastq_1: fastq_1, fastq_2: fastq_2 ] }
        .set{ ch_gba }

    /*
    ============================================================================================================================
        EXPORT TO CSV
    =============================================================================================================================
    */
    // Identify samples with/without all necessary data
    ch_gba
        .branch{ it ->
            ok: (it.assembly && it.fastq_1 && it.fastq_2)
            miss: !(it.assembly && it.fastq_1 && it.fastq_2)
        }
        .set{ ch_gba }
    // Export samples with missing data (remove previous version if it exists)
    if( gba_miss.exists() ){ gba_miss.delete() }
    ch_gba
        .miss
        .take(1)
        .map{ it.keySet().join(',') }
        .concat(ch_gba.miss.map{ it.values().join(',') })
        .collectFile(name: 'missing.csv', newLine: true, sort: 'index')
        .subscribe{ file(it).copyTo(gba_miss)  }
    // Export sample without missing data
    ch_gba
        .ok
        .take(1)
        .map{ it.keySet().join(',') }
        .concat(ch_gba.ok.map{ it.values().join(',').replace(' ','_') })
        .collectFile(name: 'gba.csv', newLine: true, sort: 'index')
        .subscribe{ file(it).copyTo(gba)  }
}

/*
=============================================================================================================================
    FUNCTIONS
=============================================================================================================================
*/
// Function for extracting string patterns
def extractId(id, pattern){
    // Check if the string matches the pattern 
    def matcher = id =~ pattern
    // If a match is found, return the matched pattern, otherwise return the string 
    return matcher.find() ? matcher.group() : id
}

// Function for identifying and formatting general bacterial analysis results from various sources (PHoeNIx, Theiaprok, RECAPP)
def formatResults(row){
    // PHoeNIx run natively
    if(row.containsKey('ID')){
        id      = row.ID
        qc      = row.containsKey('Auto_QC_Outcome') ? row.Auto_QC_Outcome : null
        species = row.containsKey('Species') ? row.Species : null
    }
    // PHoeNIx, TheiaProk, or RECAPP run on Terra
    else{
        id      = row[row.keySet().iterator().next()]
        qc      = row.containsKey('qc_outcome') ? row.qc_outcome : null // PHoeNIx only
        species = row.containsKey('species') ? row.species : ( row.containsKey('fastani_genus_species') ? row.fastani_genus_species : null )
    }
    // Clean sample names
    // remove run name from sample names
    id = id.replaceAll(/-WA.*/, '')
    // extract WA ID or WGS ID if detected
    id = extractId(id, /WA\d{7}/)
    id = extractId(id, /\d{4}JQ-\d{5}/)
    return [ id: id, qc: qc, species: species ]
}

def selectFastqs(row){
    def fastq_1 = null
    def fastq_2 = null
    if(row){
        if(row.any{ it[0] ==~ /.*_R1.*.fastq.gz/ }){
            def fastq_1Info = row.find{ it[0] ==~ /.*_R1.*.fastq.gz/ }
            def fastq_1Name = fastq_1Info[0]
            def fastq_2Name = fastq_1Name.replaceAll('R1','R2')
            if(row.any{ it[0] == fastq_2Name }){
                fastq_1 = row.find{ it[0] == fastq_1Name }[1]
                fastq_2 = row.find{ it[0] == fastq_2Name }[1]
            }
        }

    }
    return [ fastq_1: fastq_1, fastq_2: fastq_2 ]
}