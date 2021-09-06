import { Account, Connection, PublicKey } from '@solana/web3.js'
import { Market } from '@project-serum/serum'
import cors from 'cors'
import express from 'express'
import { Tedis, TedisPool } from 'tedis'
import { URL } from 'url'
import { decodeRecentEvents } from './events'
import { MarketConfig, Trade, TradeSide } from './interfaces'
import { RedisConfig, RedisStore, createRedisStore } from './redis'
import { resolutions, sleep } from './time'

async function collectEventQueue(m: MarketConfig, r: RedisConfig) {
  const store = await createRedisStore(r, m.marketName)
  const marketAddress = new PublicKey(m.marketPk)
  const programKey = new PublicKey(m.programId)
  const connection = new Connection(m.clusterUrl)
  const market = await Market.load(
    connection,
    marketAddress,
    undefined,
    programKey
  )

  async function fetchTrades(lastSeqNum?: number): Promise<[Trade[], number]> {
    const now = Date.now()
    const accountInfo = await connection.getAccountInfo(
      market['_decoded'].eventQueue
    )
    if (accountInfo === null) {
      throw new Error(
        `Event queue account for market ${m.marketName} not found`
      )
    }
    const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum)
    const takerFills = events.filter(
      (e) => e.eventFlags.fill && !e.eventFlags.maker
    )
    const trades = takerFills
      .map((e) => market.parseFillEvent(e))
      .map((e) => {
        return {
          price: e.price,
          side: e.side === 'buy' ? TradeSide.Buy : TradeSide.Sell,
          size: e.size,
          ts: now,
        }
      })
    /*
    if (trades.length > 0)
      console.log({e: events.map(e => e.eventFlags), takerFills, trades})
    */
    return [trades, header.seqNum]
  }

  async function storeTrades(ts: Trade[]) {
    if (ts.length > 0) {
      console.log(m.marketName, ts.length)
      for (let i = 0; i < ts.length; i += 1) {
        await store.storeTrade(ts[i])
      }
    }
  }

  while (true) {
    try {
      const lastSeqNum = await store.loadNumber('LASTSEQ')
      const [trades, currentSeqNum] = await fetchTrades(lastSeqNum)
      storeTrades(trades)
      store.storeNumber('LASTSEQ', currentSeqNum)
    } catch (err) {
      console.error(m.marketName, err.toString())
    }
    await sleep({
      Seconds: process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 10,
    })
  }
}

const redisUrl = new URL(process.env.REDISCLOUD_URL || 'redis://localhost:6379')
const host = redisUrl.hostname
const port = parseInt(redisUrl.port)
let password: string | undefined
if (redisUrl.password !== '') {
  password = redisUrl.password
}

const network = 'mainnet-beta'
const clusterUrl =
  process.env.RPC_ENDPOINT_URL || 'https://solana-api.projectserum.com'
const programIdV3 = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'

const nativeMarketsV3: Record<string, string> = {
  '1INCH/USDT' : "HCyhGnC77f7DaxQEvzj59g9ve7eJJXjsMYFWo4t7shcj",
  'AAVE/USDC' : "CAww1itfT8rFeTCJCLZqTq9anZ7FpC8NzULNLcJMG4Qa",
  'AKRO/USDC' : "5CZXTTgVZKSzgSA3AFMN5a2f3hmwmmJ6hU8BHTEJ3PX8",
  'ALEPH/USDC' : "GcoKtAmTy5QyuijXSmJKBtFdt99e6Buza18Js7j9AJ6e",
  'ATLAS/USDC' : "Di66GTLsV64JgCCYGVcY21RZ173BHkjJVgPyezNN7P1K",
  'BOP/USDC' : "7MmPwD1K56DthW14P1PnWZ4zPCbPWemGs3YggcT1KzsM",
  'BTC/USDT' : "C1EuT9VokAKLiW7i2ASnZUvxDoKuKkCpDDeNxAptuNe4",
  'BTC/USDC' : "A8YFbxQYFVqKZaoYJLLUVcQiWP7G2MeEgW5wsAQgMvFw",
  'CCAI/USDC' : "7gZNLDbWE73ueAoHuAeFoSu7JqmorwCLpNTBXHtYSFTa",
  'CEL/USDC' : "9MFFsTVgw6gKPZ1rpc6CSJSLaiNAonChcS7zCCMrAwEP",
  'COMP/USDC' : "Dbyf1PPrAXfMe1LdEq57QW9GY1D4nNEt2fKVGEo6S3MU",
  'COPE/USDC' : "6fc7v3PmjZG9Lk2XTot6BywGyYLkBQuzuFKd4FpCsPxk",
  'CREAM/USDC' : "7nZP6feE94eAz9jmfakNJWPwEKaeezuKKC5D1vrnqyo2",
  'CRP/USDC' : "8nXjHLfiR6wB22J7VBGeKjsRiSa54Eu7cgL17GE4kJUw",
  'DXL/USDC' : "DYfigimKWc5VhavR4moPBibx9sMcWYVSjVdWvPztBPTa",
  'ETH/USDT' : "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
  'ETH/USDC' : "4tSvZvnbyzHXLMTiFonMyxZoHmFqau1XArcRCVHLZ5gX",
  'FIDA/USDC' : "E14BKBhDWD4EuTkWj1ooZezesGxMW8LPCps4W5PuzZJo",
  'FRONT/USDC' : "9Zx1CvxSVdroKMMWf2z8RwrnrLiQZ9VkQ7Ex3syQqdSH",
  'FTR/USDC' : "4JP75nztBEo5rYhW1LTQyc4qfjPB33jMWEUvp2DGrQQR",
  'FTT/USDC' : "2Pbh1CvRVku1TgewMfycemghf6sU9EyuFDcNXqvRmSxc",
  'GRT/USDC' : "E6umfgnsastaGANjpvzb15jaXdZH1wsg4ENHARgbjqUz",
  'HGET/USDC' : "88vztw7RTN6yJQchVvxrs6oXUDryvpv9iJaFa1EEmg87",
  'HNT/USDC' : "CnUV42ZykoKUnMDdyefv5kP6nDSJf7jFd7WXAecC6LYr",
  'HOLY/USDC' : "QzB9DfWbNAUpfkwLNMLGfkK1AM2zttkMYGSwx5iCnGe",
  'HXRO/USDC' : "6Pn1cSiRos3qhBf54uBP9ZQg8x3JTardm1dL3n4p29tA",
  'KEEP/USDC' : "3rgacody9SvM88QR83GHaNdEEx4Fe2V2ed5GJp2oeKDr",
  'KIN/USDC' : "Bn6NPyr6UzrFAwC4WmvPvDr2Vm8XSUnFykM2aQroedgn",
  'LIEN/USDC' : "E6qm2xxhYa8H2YcPoydo6rFd8zgaX7cNYBoAQcw1QZ4b",
  'LIKE/USDC' : "3WptgZZu34aiDrLMUiPntTYZGNZ72yT1yxHYxSdbTArX",
  'LINK/USDC' : "3hwH1txjJVS8qv588tWrjHfRxdqNjBykM1kMcit484up",
  'LIQ/USDC' : "FLKUQGh9VAG4otn4njLPUf5gaUPx5aAZ2Q6xWiD3hH5u",
  'LQID/USDC' : "4FPFh1iAiitKYMCPDBmEQrZVgA1DVMKHZBU2R7wjQWuu",
  'LUA/USDC' : "4xyWjQ74Eifq17vbue5Ut9xfFNfuVB116tZLEpiZuAn8",
  'MAPS/USDC' : "3A8XQRWXC7BjLpgLDDBhQJLT5yPCzS16cGYRKHkKxvYo",
  'MAPSPOOL/USDC' : "7ygqNwjA94Qu8YKxB8j2ePXYEFyWLcYGUUCVzV9puAhJ",
  'MATH/USDC' : "J7cPYBrXVy8Qeki2crZkZavcojf2sMRyQU7nx438Mf8t",
  'MEDIA/USDC' : "FfiqqvJcVL7oCCu8WQUMHLUC2dnHQPAPjTdSzsERFWjb",
  'MER/USDC' : "G4LcexdCzzJUKZfqyVDQFzpkjhB1JoCNL8Kooxi9nJz5",
  'MERPOOL/USDC' : "GqQLxU1Dc6a7NYWRWdgbcGSTHirjy4quFivxXJGDzDCz",
  'MNGO/USDC' : "3d4rzwpy9iGdCZvgxcu7B1YocYffVLsQXPXkBZKt2zLc",
  'MOLA/USDC' : "HSpeWWRqBJ4HH2FPyfDhoN1AUq3gYoDenQGZASSqzYW1",
  'ORCA/USDC' : "8N1KkhaCYDpj3awD58d85n973EwkpeYnRp84y1kdZpMX",
  'OXS/USDC' : "gtQT1ipaCBC5wmTm99F9irBDhiLJCo1pbxrcFUMn6mp",
  'OXY/USDC' : "GZ3WBFsqntmERPwumFEYgrX2B7J7G11MzNZAy7Hje27X",
  'OXYPOOL/USDC' : "G1uoNqQzdasMUvXV66Eki5dwjWv5N9YU8oHKJrE4mfka",
  'PAXG/USDC' : "GJWnwZJ599xjf7cRPP93aaVKqD5xUG5PBLNypHgPxitF",
  'PERP/USDC' : "7AHAKkL94Mx2VAkQb2kk74oNsxDnQ6aab4XwKwisfFdB",
  'POLIS/USDC' : "HxFLKUAmAMLz1jtT3hbvCMELwH5H9tpM2QugP8sKyfhW",
  'PORT/USDC' : "8x8jf7ikJwgP9UthadtiGFgfFuyyyYPHL3obJAuxFWko",
  'RAY/USDC' : "2xiv8A5xrJ7RnGdxXB42uFEkYHJjszEhaJyKKt4WaLep",
  'RAYPOOL/USDC' : "3V2sfA9rCnBwjfqGca2UDxD4fVvPXW9GNAQCqAepKC9Q",
  'ROPE/USDC' : "4Sg1g8U2ZuGnGYxAhc6MmX9MX7yZbrrraPkCQ9MdCPtF",
  'RSR/USDC' : "3h5QsiZKLkmApBTsMvJBZ8fPivo9HSrmR1LaJABN9zx6",
  'SAIL/USDC' : "6hwK66FfUdyhncdQVxWFPRqY8y6usEvzekUaqtpKEKLr",
  'SAMO/USDC' : "FR3SPJmgfRSKKQ2ysUZBu7vJLpzTixXnjzb84bY3Diif",
  'SBR/USDC' : "HXBi8YBwbh4TXF6PjVw81m8Z3Cc4WBofvauj5SBFdgUs",
  'SECO/USDC' : "CjsuF2gB28KqgniogCbbpp7FDMBwAkTawEN3gYKsgfS8",
  'SLIM/USDC' : "HSfEpP3ciPBC5bBdtjBDa8BxsUW32zUzRGLPpPuDyVY4",
  'SLRS/USDC' : "2Gx3UfV831BAh8uQv1FKSPKS9yajfeeD8GJ4ZNb2o2YP",
  'SNY/USDC' : "DPfj2jYwPaezkCmUNm5SSYfkrkz8WFqwGLcxDDUsN3gA",
  'SNYPOOL/USDC' : "Eg35DZcYLx6JvZfrEAWgDPfSXJbx2N7hbEwVD56RiXnk",
  'SOL/USDC' : "9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT",
  'SOLDOGE/USDC' : "9aruV2p8cRWxybx6wMsJwPFqeN7eQVPR74RrxdM3DNdu",
  'SRM/USDC' : "ByRys5tuUWDgL73G8JBAEfkdFf8JWBzPBDHsBVQ5vbQA",
  'STEP/USDC' : "97qCB4cAVSTthvJu3eNoEx6AY6DLuRDtCoPm5Tdyg77S",
  'SUSHI/USDC' : "A1Q9iJDVVS8Wsswr9ajeZugmj64bQVCYLZQLra2TMBMo",
  'SXP/USDC' : "4LUro5jaPaTurXK737QAxgJywdhABnFAMQkXX4ZyqqaZ",
  'TOMO/USDC' : "8BdpjpSD5n3nk8DQLqPUyTZvVqFu6kcff5bzUX5dqDpy",
  'TULIP/USDC' : "8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
  'UBXT/USDC' : "2wr3Ab29KNwGhtzr5HaPCyfU1qGJzTUAN4amCLZWaD1H",
  'UNI/USDC' : "6JYHjaQBx6AtKSSsizDMwozAEDEZ5KBsSUzH7kRjGJon",
  'WOO/USDC' : "2Ux1EYeWsxywPKouRCNiALCZ1y3m563Tc4hq1kQganiq",
  'YFI/USDC' : "7qcCo8jqepnjjvB5swP4Afsr3keVBs6gNpBTNubd1Kr2",
  'renBCH/USDC' : "FS8EtiNZCH72pAK83YxqXaGAgk3KKFYphiTcYA2yRPis",
  'renBTC/USDC' : "74Ciu5yRzhe8TFTHvQuEVbFZJrbnCMRoohBK33NNiPtv",
  'renDOGE/USDC' : "5FpKCWYXgHWZ9CdDMHjwxAfqxJLdw2PRXuAmtECkzADk",
  'renLUNA/USDC' : "CxDhLbbM9uAA2AXfSPar5qmyfmC69NLj3vgJXYAsSVBT",
  'renZEC/USDC' : "2ahbUT5UryyRVxPnELtTmDLLneN26UjBQFgfMVvbWDTb",
}

const symbolsByPk = Object.assign(
  {},
  ...Object.entries(nativeMarketsV3).map(([a, b]) => ({ [b]: a }))
)

function collectMarketData(programId: string, markets: Record<string, string>) {
  Object.entries(markets).forEach((e) => {
    const [marketName, marketPk] = e
    const marketConfig = {
      clusterUrl,
      programId,
      marketName,
      marketPk,
    } as MarketConfig
    collectEventQueue(marketConfig, { host, port, password, db: 0 })
  })
}

collectMarketData(programIdV3, nativeMarketsV3)

const max_conn = parseInt(process.env.REDIS_MAX_CONN || '') || 200
const redisConfig = { host, port, password, db: 0, max_conn }
const pool = new TedisPool(redisConfig)

const app = express()
const corsOptions ={
  origin:'*', 
  credentials:true,            //access-control-allow-credentials:true
  optionSuccessStatus:200,
}
app.use(cors(corsOptions))

app.get('/tv/config', async (req, res) => {
  const response = {
    supported_resolutions: Object.keys(resolutions),
    supports_group_request: false,
    supports_marks: false,
    supports_search: true,
    supports_timescale_marks: false,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/symbols', async (req, res) => {
  const symbol = req.query.symbol as string
  const response = {
    name: symbol,
    ticker: symbol,
    description: symbol,
    type: 'Spot',
    session: '24x7',
    exchange: 'Cedros',
    listed_exchange: 'Cedros',
    timezone: 'Etc/UTC',
    has_intraday: true,
    supported_resolutions: Object.keys(resolutions),
    minmov: 1,
    pricescale: 100,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/history', async (req, res) => {
  // parse
  const marketName = req.query.symbol as string
  const marketPk = nativeMarketsV3[marketName]
  const resolution = resolutions[req.query.resolution as string] as number
  let from = parseInt(req.query.from as string) * 1000
  let to = parseInt(req.query.to as string) * 1000

  // validate
  const validSymbol = marketPk != undefined
  const validResolution = resolution != undefined
  const validFrom = true || new Date(from).getFullYear() >= 2021
  if (!(validSymbol && validResolution && validFrom)) {
    const error = { s: 'error', validSymbol, validResolution, validFrom }
    console.error({ marketName, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const conn = await pool.getTedis()
    try {
      const store = new RedisStore(conn, marketName)

      // snap candle boundaries to exact hours
      from = Math.floor(from / resolution) * resolution
      to = Math.ceil(to / resolution) * resolution

      // ensure the candle is at least one period in length
      if (from == to) {
        to += resolution
      }
      const candles = await store.loadCandles(resolution, from, to)
      const response = {
        s: 'ok',
        t: candles.map((c) => c.start / 1000),
        c: candles.map((c) => c.close),
        o: candles.map((c) => c.open),
        h: candles.map((c) => c.high),
        l: candles.map((c) => c.low),
        v: candles.map((c) => c.volume),
      }
      res.set('Cache-control', 'public, max-age=1')
      res.send(response)
      return
    } finally {
      pool.putTedis(conn)
    }
  } catch (e) {
    console.error({ req, e })
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

app.get('/trades/address/:marketPk', async (req, res) => {
  // parse
  const marketPk = req.params.marketPk as string
  const marketName = symbolsByPk[marketPk]

  // validate
  const validPk = marketName != undefined
  if (!validPk) {
    const error = { s: 'error', validPk }
    console.error({ marketPk, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const conn = await pool.getTedis()
    try {
      const store = new RedisStore(conn, marketName)
      const trades = await store.loadRecentTrades()
      const response = {
        success: true,
        data: trades.map((t) => {
          return {
            market: marketName,
            marketAddress: marketPk,
            price: t.price,
            size: t.size,
            side: t.side == TradeSide.Buy ? 'buy' : 'sell',
            time: t.ts,
            orderId: '',
            feeCost: 0,
          }
        }),
      }
      res.set('Cache-control', 'public, max-age=5')
      res.send(response)
      return
    } finally {
      pool.putTedis(conn)
    }
  } catch (e) {
    console.error({ req, e })
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

const httpPort = parseInt(process.env.PORT || '5000')
app.listen(httpPort)
