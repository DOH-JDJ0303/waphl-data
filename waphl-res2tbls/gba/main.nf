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
        .map{ [ it.id, it ] }
        .groupTuple(by: 0) // group by sample id
        .map{ id, data -> [ id: id ] + data.max{ it.timestamp } } // select most recent version of each sample
        .unique() // remove duplicates
        .filter{ it.qc == 'FAIL' ? false : true } // select only samples that pass QC (if QC data is available)
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
        .join( ch_gba.map{ [ it.id, it ] }, by: 0, remainder: true )
        .filter{ id, fastq, data -> data ? ( data.species ? true : false ) : false  } // must contain species value
        .map{ id, fastqs, data -> data + selectFastqs( fastqs ) }
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
        .map{ [ it.id_alt, [ file: it.current, timestamp: it.timestamp ]  ] }
        .unique()
        .groupTuple(by: 0)
        .map{ id, data -> [ id, data.max{ it.timestamp } ] }
        .join( ch_gba.map{ [ it.id, it ] }, by: 0, remainder: true )
        .filter{ id, fasta, data -> data ? ( data.species ? true : false ) : false  } // must contain species value
        .map{ id, fasta, data -> data + [ assembly: fasta ? fasta.file : null ] }
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
    // Define column name options for each result
    // Note: This only includes results that have been validated or metrics used to evaluate result quality. Exclusion of a column from this list is not necessarily a mistake.
    cols_key = [
        qc: [ 'Auto_QC_Outcome', 'qc_outcome', 'aa_qc_check' ],
        qc_reason:     [ 'Auto_QC_Failure_Reason', 'qc_reason', 'aa_qc_alert' ],
        read_depth:    [ 'Estimated_Coverage', 'estimated_coverage', 'est_coverage_clean' ],
        assembly_len:  [ 'Genome_Length', 'genome_length', 'assembly_length' ],
        n_contigs:     [ '#_of_Scaffolds_>500bp', 'scaffold_count', 'number_contigs' ],
        per_gc:        [ 'GC_%', 'gc_percent', 'quast_gc_percent' ],
        species:       [ 'Species', 'species', 'fastani_genus_species' ],
        species_conf:  [ 'Taxa_Confidence', 'taxa_confidence', 'fastani_ani_estimate' ],
        mlst_1:        [ 'MLST_1', 'mlst_1' ],
        mlst_2:        [ 'MLST_2', 'mlst_2' ],
        mlst_1_scheme: [ 'MLST_Scheme_1', 'mlst_scheme_1' ],
        mlst_2_scheme: [ 'MLST_Scheme_2', 'mlst_scheme_2' ],
        beta_amr:      [ 'GAMMA_Beta_Lactam_Resistance_Genes', 'beta_lactam_resistance_genes' ],
        other_amr:     [ 'GAMMA_Other_AR_Genes', 'other_ar_genes' ],
        plasmids:      [ 'Plasmid_Incompatibility_Replicons','plasmid_incompatibility_replicons' ],
        vir_genes:     [ 'Hypervirulence_Genes', 'hypervirulence_genes' ],
    ]

    def results = [ id: row.containsKey('ID') ? row.ID : row[row.keySet().iterator().next()] ]
    cols_key.each{ key, value -> col = value.findAll{ row.containsKey( it ) }
                                 results[key] = row[col[0]] ? row[col[0]].replaceAll(',',';').replaceAll(' ','_') : null  }

    results.id = results.id.replaceAll(/-WA.*/, '')
    results.id =  extractId(results.id, /WA\d{7}/) 
    results.id = extractId(results.id, /\d{4}JQ-\d{5}/)

    return results
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