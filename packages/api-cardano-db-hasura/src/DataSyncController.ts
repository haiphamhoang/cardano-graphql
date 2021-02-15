import axios, { AxiosInstance } from 'axios'
import { DataFetcher } from '@cardano-graphql/util'
import { Asset } from './graphql_types'
import { dummyLogger, Logger } from 'ts-log'
import { Db } from './Db'
import { HasuraClient } from './HasuraClient'

export interface Signature {
  signature: string
  publicKey: string
}

export interface AssetMetadata {
  description: {
    value: string
    anSignatures: Signature[]
  }
  name: {
    value: string
    anSignatures: Signature[]
  }
  owner?: Signature
  preImage?: {
    value: string
    hashFn: string
  }
  subject: string
}

export class DataSyncController {
  private axiosClient: AxiosInstance
  private assetSynchronizer: DataFetcher<number>
  private metadataSynchronizer: DataFetcher<number>
  private metadataSynchronizerRetries: DataFetcher<number>

  constructor (
    readonly hasuraClient: HasuraClient,
    readonly db: Db,
    pollingInterval: {
      metadataSync: number
      metadataSyncRetry: number
    },
    private logger: Logger = dummyLogger,
    private metadataServerUri?: string
  ) {
    if (this.metadataServerUri) {
      this.axiosClient = axios.create({
        baseURL: metadataServerUri
      })
      this.metadataSynchronizer = new DataFetcher<number>(
        'MetadataSynchronizer',
        async () => {
          const assets = await this.hasuraClient.getIdsOfAssetsWithoutMetadata({ _lte: 5 })
          this.logger.debug('Assets missing metadata', { module: 'DataSyncController', value: assets.length })
          if (assets.length > 0) {
            const newMetadata = await this.getAssetMetadata(assets)
            this.logger.debug('Metadata fetched', { module: 'DataSyncController', value: newMetadata.length })
            if (newMetadata.length > 0) {
              for (const metadata of newMetadata) {
                await this.hasuraClient.addMetadata(metadata)
              }
            }
            for (const asset of assets) {
              await this.hasuraClient.incrementMetadataFetchAttempts(asset.assetId)
            }
          }
          return assets.length
        },
        pollingInterval.metadataSync,
        this.logger
      )
      this.metadataSynchronizerRetries = new DataFetcher<number>(
        'MetadataSynchronizerRefresh',
        async () => {
          const assets = await this.hasuraClient.getAssetsIncMetadata()
          this.logger.debug('All assets', { module: 'DataSyncController', value: assets.length })
          if (assets.length > 0) {
            const newMetadata = await this.getAssetMetadata(assets)
            newMetadata.filter(metadata => {
              const { description, name } = assets.find(asset => asset.assetId === metadata.subject)
              return metadata.name.value !== name || metadata.description.value !== description
            })
            this.logger.debug('Metadata with updates to apply', { module: 'DataSyncController', value: newMetadata.length })
            if (newMetadata.length > 0) {
              for (const metadata of newMetadata) {
                await this.hasuraClient.addMetadata(metadata)
              }
            }
            for (const asset of assets) {
              await this.hasuraClient.incrementMetadataFetchAttempts(asset.assetId)
            }
          }
          return assets.length
        },
        pollingInterval.metadataSyncRetry,
        this.logger
      )
    }
    this.assetSynchronizer = new DataFetcher<number>(
      'AssetTableSynchronizer',
      async () => {
        const distinctAssetsInTokens = await this.hasuraClient.getDistinctAssetsInTokens()
        this.logger.debug('distinct asset IDs from tokens', { module: 'DataSyncController', value: distinctAssetsInTokens.length })
        const assetIds = await this.hasuraClient.getAssetIds()
        this.logger.debug('fetched asset IDs', { module: 'DataSyncController', value: assetIds.length })
        const diff = distinctAssetsInTokens.filter(token => !assetIds.includes(token.assetId))
        this.logger.debug('asset IDs diff', { module: 'DataSyncController', value: diff.length })
        if (diff.length > 0) {
          await this.hasuraClient.insertAssets(diff)
          this.logger.debug('synchronised assets table from tokens', { module: 'DataSyncController', value: diff.length })
        }
        return diff.length
      },
      60 * 1000,
      this.logger
    )
  }

  private async getAssetMetadata (assets: Asset[]): Promise<AssetMetadata[]> {
    try {
      const response = await this.axiosClient.post('query', {
        subjects: assets.map(asset => asset.assetId),
        properties: ['description', 'name']
      })
      return response.data.subjects
    } catch (error) {
      this.logger.error(error.message)
      throw error
    }
  }

  public async initialize () {
    this.logger.info('Initializing', { module: 'DataSyncController' })
    await this.assetSynchronizer.initialize()
    if (this.metadataServerUri) {
      await this.metadataSynchronizer.initialize()
      await this.metadataSynchronizerRetries.initialize()
    }
    this.logger.info('Initialized', { module: 'DataSyncController' })
  }

  public async shutdown () {
    this.logger.info('Shutting down', { module: 'DataSyncController' })
    if (this.metadataServerUri) {
      await this.metadataSynchronizer.shutdown()
      await this.metadataSynchronizerRetries.shutdown()
    }
    await this.assetSynchronizer.shutdown()
    this.logger.info('Shutdown complete', { module: 'DataSyncController' })
  }
}
