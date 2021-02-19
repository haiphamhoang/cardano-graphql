
export interface Config {
  cardanoCliPath: string,
  cardanoNodeSocketPath: string,
  db: {
    database: string,
    host: string,
    password: string,
    port: number
    user: string,
  },
  genesis: {
    byronPath: string,
    shelleyPath: string
  },
  hasuraCliPath: string,
  hasuraUri: string,
  jqPath: string,
  metadataServerUri: string,
  pollingInterval: {
    adaSupply: number
    metadataSync: number
    metadataSyncRetry: number
  }
}
