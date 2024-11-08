#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

def collect_files(dir){
    count = 0
    def fileList = []
    file(dir).eachFileRecurse { f -> 
                                if(f.isFile()){
                                    print count++
                                    fileList << f 
                                }
                            }
    return fileList
}

def run_dir         = file(params.run_dir)
def manifest        = run_dir.resolve("manifest.csv").splitCsv(header: true)
def files           = collect_files(run_dir).collect{ f -> f.toString().replace(run_dir.toString(), "" ) }
def run_files       = files
                          .findAll{ f -> manifest.sample.any{ s -> !(f ==~ /.*${s}.*/) } }
                          .unique()
def sample_files    = files
                          .findAll{ f -> manifest.sample.any{ s -> f ==~ /.*${s}.*/ } }
                          .collect{ f -> manifest.sample.inject(f) { nf, s -> nf.replaceAll(s, "<sample>") } }
                          .unique()

config_file = file("${params.workflow}_retentionSchema.config")
config_file.text = """\
${params.workflow} {
    run_files    = [
        ${run_files.collect{ f -> "'${f}'" }.join(",\n        ")}]
    sample_files = [
        ${sample_files.collect{ f -> "'${f}'" }.join(",\n        ")}
                  ]
}
"""
.stripIndent()
