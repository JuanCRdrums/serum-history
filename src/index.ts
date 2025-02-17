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
  try{
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
    try{
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
      catch (err){
        console.error(err.toString())
        return [[],0]
      }
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
        Seconds: process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 20,
      })
    }
  } catch (err) {
    console.error(m.marketName, err.toString())
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
  'AART/USDC' : "HubRne1PhTa8ufcGRmS6sMJpbB5yY3BEykUuw8xXoAyy",
  'ABR/USDC' : "FrR9FBmiBjm2GjLZbfnCcgkbueUJ78NbBx1qcQKPUQe8",
  'ACM/USDC' : "94CZWJMy7Jmc7gSM288WmFfse596H12KCKgwMARkyvhw",
  'AKRO/USDC' : "5CZXTTgVZKSzgSA3AFMN5a2f3hmwmmJ6hU8BHTEJ3PX8",
  'ALEPH/USDC' : "GcoKtAmTy5QyuijXSmJKBtFdt99e6Buza18Js7j9AJ6e",
  'ALM/USDC' : "DNxn3qM61GZddidjrzc95398SCWhm5BUyt8Y8SdKYr8W",
  'APT/USDC' : "ATjWoJDChATL7E5WVeSk9EsoJAhZrHjzCZABNx3Miu8B",
  'ATLAS/USDC' : "Di66GTLsV64JgCCYGVcY21RZ173BHkjJVgPyezNN7P1K",
  'ATS/USDC' : "6NvErACd7kKz5uPDAfXKahW9sFooUJvtfRh5Kzq6GH9z",
  'AURY/USDC' : "461R7gK9GK1kLUXQbHgaW9L6PESQFSLGxKXahvcHEJwD",
  'AVAX/USDC' : "E8JQstcwjuqN5kdMyUJLNuaectymnhffkvfg1j286UCr",
  'AXS/USDC' : "HZCheduA4nsSuQpVww1TiyKZpXSAitqaXxjBD2ymg22X",
  'BASIC/USDC' : "HCWgghHfDefcGZsPsLAdMP3NigJwBrptZnXemeQchZ69",
  'BLOCK/USDC' : "2b6GbUbY979QhRoWb2b9F3vNi7pcCGPDivuiKPHC56zY",
  'BLT/USDC' : "Bo7aDwpZ8xHh6YGbiNMhcrHWpoB1f6ELgbaGk6M6o8Yn",
  'BMBO/USDC' : "8dpaLWWPv6vFong1D8gHFDmYzHQreXuKcui3XCKBACCj",
  'BNB/USDC' : "3zzTxtDCt9PimwzGrgWJEbxZfSLetDMkdYegPanGNpMf",
  'BOT/USDC' : "EbtdM5qD52MBb8mKLXfFc2G8wpzP4mMhtsdAJzvZ9tfC",
  'BOKU/USDC' : "Dvm8jjdAy8uyXn9WXjS2p1mcPeFTuYS6yW2eUL9SJE8p",
  'BOP/USDC' : "7MmPwD1K56DthW14P1PnWZ4zPCbPWemGs3YggcT1KzsM",
  'BTC/USDT' : "C1EuT9VokAKLiW7i2ASnZUvxDoKuKkCpDDeNxAptuNe4",
  'BTC/USDC' : "A8YFbxQYFVqKZaoYJLLUVcQiWP7G2MeEgW5wsAQgMvFw",
  'BTC/UST' : "DLuhwVesu1GQBMzvEEcGeVFyLVSuc7id7D3Axq2gw2Dm",
  'CATO/USDC' : "9fe1MWiKqUdwift3dEpxuRHWftG72rysCRHbxDy6i9xB",
  'CAVE/USDC' : "KrGK6ZHyE7Nt35D7GqAKJYAYUPUysGtVBgTXsJuAxMT",
  'CEL/USDC' : "9MFFsTVgw6gKPZ1rpc6CSJSLaiNAonChcS7zCCMrAwEP",
  'CHEEMS/USDC' : "5WVBCaUPZF4HP3io9Z56N71cPMJt8qh3c4ZwSjRDeuut",
  'CHICKS/USDC' : "Eg8a9ZicLPSyak4CiXfiMeJK6jmHq57Xx5ag5GY6vcDj",
  'CMFI/USDC' : "3Mf3bxVS2zLW3bbr9BNbqdiizaUwCGwoi3xhrAXfbFnW",
  'COMP/USDC' : "Dbyf1PPrAXfMe1LdEq57QW9GY1D4nNEt2fKVGEo6S3MU",
  'COPE/USDC' : "6fc7v3PmjZG9Lk2XTot6BywGyYLkBQuzuFKd4FpCsPxk",
  'CREAM/USDC' : "7nZP6feE94eAz9jmfakNJWPwEKaeezuKKC5D1vrnqyo2",
  'CRP/USDC' : "93mtNf4qzvytwp5sWrSC7JNUccPAomEE39ztErUq5V3F",
  'CRWNY/USDC' : "H8GSFzSZmPNs4ANW9dPd5XTgrzWkta3CaT57TgWYs7SV",
  'CWAR/USDC' : "CDYafmdHXtfZadhuXYiR7QaqmK9Ffgk2TA8otUWj9SWz",
  'CWM/USDC' : "E8CLGjBzJD7TPgjfcWm8dvpXhz5pvdjcPxhG7qi7Y5un",
  'CYS/USDC' : "6V6y6QFi17QZC9qNRpVp7SaPiHpCTp2skbRQkUyZZXPW",
  'DAPE/USDC' : "CQQAHgfsu5uFTPzwewZ7QrLLSPk1K9MmqnuxiwmPDBzN",
  'DATE/USDC' : "3jszawPiXjuqg5MwAAHS8wehWy1k7de5u5pWmmPZf6dM",
  'DFL/USDC' : "9UBuWgKN8ZYXcZWN67Spfp3Yp67DKBq1t31WLrVrPjTR",
  'DIO/USDT' : "AhLNACGvMQk6vtDq1mdcG5gidbnH558rAD6fhxBpcKUi",
  'DXL/USDC' : "DYfigimKWc5VhavR4moPBibx9sMcWYVSjVdWvPztBPTa",
  'DYDX/USDC' : "GNmTGd6iQvQApXgsyvHepDpCnvdRPiWzRr8kzFEMMNKN",
  'ETH/USDT' : "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
  'ETH/USDC' : "4tSvZvnbyzHXLMTiFonMyxZoHmFqau1XArcRCVHLZ5gX",
  'agEUR/USDC' : "eYJn1SMJhM6G9suhhVYtt28SbAXCswqKe6UKbq7pYLk",
  'FAB/USDC' : "Cud48DK2qoxsWNzQeTL5D8sAiHsGwG8Ev1VMNcYLayxt",
  'FCON/USDC' : "GnZuMX8RFGzasx4CoJaTSseGExqdva9KYY4Pw6QpGjS2",
  'FIDA/USDC' : "E14BKBhDWD4EuTkWj1ooZezesGxMW8LPCps4W5PuzZJo",
  'FLOOF/USDC' : "BxcuT1p8FK9cFak4Uuf5nmoAZ7nQGu7FerCMESGqxF7b",
  'FLWR/USDC' : "CptfkZ4ZVDosgjLAoxNApRUEBg82qQKfiqpWiJwc3iRF",
  'FRIES/USDC' : "8DKD5dKmmwparxCkpV2BQFTxt164rfadK8kX3at6hWUB",
  'FRKT/USDC' : "8inqBe7D12XJ6tMAzpLCGYpjazWFXG1Ue5q3UZ6X1FM3",
  'FRONT/USDC' : "9Zx1CvxSVdroKMMWf2z8RwrnrLiQZ9VkQ7Ex3syQqdSH",
  'FTR/USDC' : "4JP75nztBEo5rYhW1LTQyc4qfjPB33jMWEUvp2DGrQQR",
  'FTT/USDC' : "2Pbh1CvRVku1TgewMfycemghf6sU9EyuFDcNXqvRmSxc",
  'GARI/USDC' : "CM1i8dGf2tkAFfKQGuS1gPAaMJmgGgpfCUgjc8oRSrtF",
  'GENE/USDC' : "FwZ2GLyNNrFqXrmR8Sdkm9DQ61YnQmxS6oobeH3rrLUM",
  'GGSG/USDC' : "2x6dmFevT3MGCZYPE2eagautjyQFVq4YwHsKow5bCVCs",
  'GOD/MIX' : "ABdKNuKrgdTjvy5kW1cmrC826NShzu4FYAfPSHRQJkm3",
  'GOFX/USDC' : "2wgi2FabNsSDdb8dke9mHFB67QtMYjYa318HpSqyJLDD",
  'GRAPE/USDC' : "72aW3Sgp1hMTXUiCq8aJ39DX2Jr7sZgumAvdLrLuCMLe",
  'HGET/USDC' : "88vztw7RTN6yJQchVvxrs6oXUDryvpv9iJaFa1EEmg87",
  'HNT/USDC' : "CnUV42ZykoKUnMDdyefv5kP6nDSJf7jFd7WXAecC6LYr",
  'HOLY/USDC' : "QzB9DfWbNAUpfkwLNMLGfkK1AM2zttkMYGSwx5iCnGe",
  'HXRO/USDC' : "6Pn1cSiRos3qhBf54uBP9ZQg8x3JTardm1dL3n4p29tA",
  'IN/USDC': "49vwM54DX3JPXpey2daePZPmimxA4CrkXLZ6E1fGxx2Z",
  'ISOLA/USDT' : "42QVcMqoXmHT94zaBXm9KeU7pqDfBuAPHYN9ADW8weCF",
  'IVN/USDC' : "4JDhmLVobWpUaV8tr3ZGAXmSp3vMf24a2D2dVfoH1E5T",
  'JET/USDC' : "6pQMoHDC2o8eeFxyTKtfnsr8d48hKFWsRpLHAqVHH2ZP",
  'JSOL/USDC' : "8mQ3nNCdcwSHkYwsRygTbBFLeGPsJ4zB2zpEwXmwegBh",
  'KEEP/USDC' : "3rgacody9SvM88QR83GHaNdEEx4Fe2V2ed5GJp2oeKDr",
  'KIN/USDC' : "Bn6NPyr6UzrFAwC4WmvPvDr2Vm8XSUnFykM2aQroedgn",
  'KKO/USDC' : "9zR51YmUq2Tzccaq4iXXWDKbNy2TkEyPmoqCsfpjw2bc",
  'KURO/USDC' : "9oXkdAWFyjDH8BbYrDVJ77r6GWPmUWo9ZYYpE25SZ2td",
  'LARIX/USDC' : "DE6EjZoMrC5a3Pbdk8eCMGEY9deeeHECuGFmEuUpXWZm",
  'LDO/USDC' : "5kdErBywDQzHkJedkFoAZp4L2r7GdX4gwi79aCMNoNXs",
  'LIKE/USDC' : "3WptgZZu34aiDrLMUiPntTYZGNZ72yT1yxHYxSdbTArX",
  'LINK/USDC' : "3hwH1txjJVS8qv588tWrjHfRxdqNjBykM1kMcit484up",
  'LIQ/USDC' : "D7p7PebNjpkH6VNHJhmiDFNmpz9XE7UaTv9RouxJMrwb",
  'LQID/USDC' : "4FPFh1iAiitKYMCPDBmEQrZVgA1DVMKHZBU2R7wjQWuu",
  'LUA/USDC' : "4xyWjQ74Eifq17vbue5Ut9xfFNfuVB116tZLEpiZuAn8",
  'MANA/USDC' : "7GSn6KQRasgPQCHwCbuDjDCsyZ3cxVHKWFmBXzJUUW8P",
  'MAPS/USDC' : "3A8XQRWXC7BjLpgLDDBhQJLT5yPCzS16cGYRKHkKxvYo",
  'MAPSPOOL/USDC' : "7ygqNwjA94Qu8YKxB8j2ePXYEFyWLcYGUUCVzV9puAhJ",
  'MATH/USDC' : "J7cPYBrXVy8Qeki2crZkZavcojf2sMRyQU7nx438Mf8t",
  'MATIC/USDC' : "5WRoQxE59966N2XfD2wYy1uhuyKeoVJ9NBMH6r6RNYEF",
  'MBS/USDC' : "9sUSmgx78tt692hzwiRdBdfwjxPF6nsYeJfPCrTz6vxm",
  'MEDIA/USDC' : "FfiqqvJcVL7oCCu8WQUMHLUC2dnHQPAPjTdSzsERFWjb",
  'MER/USDC' : "G4LcexdCzzJUKZfqyVDQFzpkjhB1JoCNL8Kooxi9nJz5",
  'MERPOOL/USDC' : "GqQLxU1Dc6a7NYWRWdgbcGSTHirjy4quFivxXJGDzDCz",
  'MIX/USDC' :  "4k5mp4H52Zy6YSZPuVEfiXk5K45okXDbBxc5nfAhNRou",
  'MNDE/mSOL' : "AVxdeGgihchiKrhWne5xyUJj7bV2ohACkQFXMAtpMetx",
  'MNGO/USDC' : "3d4rzwpy9iGdCZvgxcu7B1YocYffVLsQXPXkBZKt2zLc",
  'MOLA/USDC' : "HSpeWWRqBJ4HH2FPyfDhoN1AUq3gYoDenQGZASSqzYW1",
  'mSOL/USDC' : "6oGsL2puUgySccKzn9XA9afqF217LfxP5ocq4B3LWsjy",
  'NFD/USDC' : "EtEKBLqLfPcm8mXn5JdzY9rMghHnTMxpWwsdASasAMNa",
  'NOS/USDC' : "8XaEfk3TURqgrJZvdJWrPLqyXSCmJ9MfSwZrkaYmsG7r", 
  'OOGI/USDC' : "ANUCohkG9gamUn6ofZEbnzGkjtyMexDhnjCwbLDmQ8Ub",
  'ORCA/USDC' : "8N1KkhaCYDpj3awD58d85n973EwkpeYnRp84y1kdZpMX",
  'OXS/USDC' : "gtQT1ipaCBC5wmTm99F9irBDhiLJCo1pbxrcFUMn6mp",
  'OXY/USDC' : "GZ3WBFsqntmERPwumFEYgrX2B7J7G11MzNZAy7Hje27X",
  'OXYPOOL/USDC' : "G1uoNqQzdasMUvXV66Eki5dwjWv5N9YU8oHKJrE4mfka",
  'PAXG/USDC' : "GJWnwZJ599xjf7cRPP93aaVKqD5xUG5PBLNypHgPxitF",
  'PERP/USDC' : "7AHAKkL94Mx2VAkQb2kk74oNsxDnQ6aab4XwKwisfFdB",
  'PEOPLE/USDC' : "GsWEL352sYgQC3uAVKgEQz2TtA1RA5cgNwUQahyzwJyz",
  'POLIS/USDC' : "HxFLKUAmAMLz1jtT3hbvCMELwH5H9tpM2QugP8sKyfhW",
  'PORT/USDC' : "8x8jf7ikJwgP9UthadtiGFgfFuyyyYPHL3obJAuxFWko",
  'PRISM/USDC' : "2MvXnxngd1gKp6gE8Q63wiPHSpveWcx8x4wf43VpyiA6",
  'PRMS/USDC' : "3djZW2bRhEpYuu5zUs4UJfQVJ8aoruU82EpF5NWhQLmn", 
  'PRT/USDC' : "CsNZMtypiGgxm6JrmYVJWnLnJNsERrmT3mQqujLsGZj",
  'PSK/USDC' : "3XRsg7YRVcvZme9rHpATL55WPHrnGXvVL8E7ycNNFu4f",
  'PSY/USDC' : "9WDPi1uZVxBwZY4NXy7A3nGfxAzxvNaS56iHk3cBhQ3U",
  'RAY/USDC' : "2xiv8A5xrJ7RnGdxXB42uFEkYHJjszEhaJyKKt4WaLep",
  'RAYPOOL/USDC' : "3V2sfA9rCnBwjfqGca2UDxD4fVvPXW9GNAQCqAepKC9Q",
  'REAL/USDC' : "AU8VGwd4NGRbcMz9LT6Fu2LP69LPAbWUJ6gEfEgeYM33",
  'RIN/USDC' : "7gZNLDbWE73ueAoHuAeFoSu7JqmorwCLpNTBXHtYSFTa",
  'ROLL/USDC' : "7S1pauEg5VWSXeEFQk9WZqBkGozZFUGiEAS5nub29krn",
  'ROPE/USDC' : "4Sg1g8U2ZuGnGYxAhc6MmX9MX7yZbrrraPkCQ9MdCPtF",
  'RSR/USDC' : "3h5QsiZKLkmApBTsMvJBZ8fPivo9HSrmR1LaJABN9zx6",
  'RUN/USDC' : "HCvX4un57v1SdYQ2LFywaDYyZySqLHMQ5cojq5kQJM3y",
  'SAIL/USDC' : "6hwK66FfUdyhncdQVxWFPRqY8y6usEvzekUaqtpKEKLr",
  'SAMO/USDC' : "FR3SPJmgfRSKKQ2ysUZBu7vJLpzTixXnjzb84bY3Diif",
  'SAND/USDC' : "3FE2g3cadTJjN3C7gNRavwnv7Yh9Midq7h9KgTVUE7tR",
  'SBR/USDC' : "HXBi8YBwbh4TXF6PjVw81m8Z3Cc4WBofvauj5SBFdgUs",
  'SCY/USDC' : "DR8V2wUCSFKCGjML6AZvUB2eYWDPVQ5xju3DeXGgpSaB",
  'SECO/USDC' : "CjsuF2gB28KqgniogCbbpp7FDMBwAkTawEN3gYKsgfS8",
  'SHDW/USDC' : "CVJVpXU9xksCt2uSduVDrrqVw6fLZCAtNusuqLKc5DhW",
  'SHIB/USDC' : "Er7Jp4PADPVHifykFwbVoHdkL1RtZSsx9zGJrPJTrCgW",
  'SHILL/USDC' : "3KNXNjf1Vp3V5gYPjwnpALYCPhWpRXsPPC8CWBXqmnnN",
  'SLC/USDC' : "DvmDTjsdnN77q7SST7gngLydP1ASNNpUVi4cNfU95oCr",
  'SLIM/SOL' : "GekRdc4eD9qnfPTjUMK5NdQDho8D9ByGrtnqhMNCTm36",
  'SLND/USDC' : "F9y9NM83kBMzBmMvNT18mkcFuNAPhNRhx7pnz9EDWwfv",
  'SLRS/USDC' : "2Gx3UfV831BAh8uQv1FKSPKS9yajfeeD8GJ4ZNb2o2YP",
  'SNY/USDC' : "DPfj2jYwPaezkCmUNm5SSYfkrkz8WFqwGLcxDDUsN3gA",
  'SNYPOOL/USDC' : "Eg35DZcYLx6JvZfrEAWgDPfSXJbx2N7hbEwVD56RiXnk",
  'SOL/USDC' : "9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT",
  'SOL/UST' : "7TeAZYGC6VB1q8UFR5tMbQdGjJVpmLXYe3bnjCZgj2FM",
  'SOLAPE/USDC' : "4zffJaPyeXZ2wr4whHgP39QyTfurqZ2BEd4M5W6SEuon",
  'SOLAR/USDC' : "BHfFJM36MirbBtLCcnZokwRvxUPxk7Ez6EAT6k44q6Go",
  'SOLDOGE/USDC' : "9aruV2p8cRWxybx6wMsJwPFqeN7eQVPR74RrxdM3DNdu",
  'SOLX/USDC' : "97tCGtxWywJXkudCtderXZoQpTWXyWDQoLq13dqexScH",
  'SONAR/USDC' : "9YdVSNrDsKDaGyhKL2nqEFKvxe3MSqMjmAvcjndVg1kj",
  'SRM/USDC' : "ByRys5tuUWDgL73G8JBAEfkdFf8JWBzPBDHsBVQ5vbQA",
  'SSU/SOL' : "DbVTo7vZQz84zosY2yfytQK8tDbf9ofRNkgKepQcjNhZ",
  'STARS/USDC' : "DvLrUbE8THQytBCe3xrpbYadNRUfUT7SVCm677Nhrmby",
  'STEP/USDC' : "97qCB4cAVSTthvJu3eNoEx6AY6DLuRDtCoPm5Tdyg77S",
  'STR/USDC' : "6vXecj4ipEXChK9uPAd5giWn6aB3fn5Lbu4eVMLX7rRU",
  'stSOL/USDC' : "5F7LGsP1LPtaRV7vVKgxwNYX4Vf22xvuzyXjyar7jJqp",
  'SUNNY/USDC' : "Aubv1QBFh4bwB2wbP1DaPW21YyQBLfgjg8L4PHTaPzRc",
  'SUSHI/USDC' : "A1Q9iJDVVS8Wsswr9ajeZugmj64bQVCYLZQLra2TMBMo",
  'SVT/USDC' : "HuFKVQNyB177c9DiocQksYzBCtHMRUP5bBXZJzuLvYQm",
  'SWOLEDOGE/USDC' : "3SGeuz8EXsyFo4HHWXQsoo8r4r5RdZkt7TuuTZiVbKc8",
  'SXP/USDC' : "4LUro5jaPaTurXK737QAxgJywdhABnFAMQkXX4ZyqqaZ",
  'SYP/USDC' : "9cuBrXXSH9Uw51JB9odLqEyeF5RQSeRpcfXbEW2L8X6X",
  'TINY/USDC' : "998TAsB7D2FnFrri5YSVHfw7Ajgckz5skdu1qPPMFVSt",
  'TOMO/USDC' : "8BdpjpSD5n3nk8DQLqPUyTZvVqFu6kcff5bzUX5dqDpy",
  'TOX/USDC' : "21PEcBLwqFceMAfPB7b8Rt224RpH6UuUrwqNTSqdPse5",
  'TTT/USDC' : "2sdQQDyBsHwQBRJFsYAGpLZcxzGscMUd5uxr8jowyYHs",
  'TULIP/USDC' : "8GufnKq7YnXKhnB3WNhgy5PzU9uvHbaaRrZWQK6ixPxW",
  'UBXT/USDC' : "2wr3Ab29KNwGhtzr5HaPCyfU1qGJzTUAN4amCLZWaD1H",
  'USDT/USDC' : "77quYg4MGneUdjgXCunt9GgM1usmrxKY31twEy3WHwcS",
  'UST/USDC' : "EERNEEnBqdGzBS8dd46wwNY5F2kwnaCQ3vsq2fNKGogZ",
  'UNI/USDC' : "6JYHjaQBx6AtKSSsizDMwozAEDEZ5KBsSUzH7kRjGJon",
  'UPS/USDC' : "DByPstQRx18RU2A8DH6S9mT7bpT6xuLgD2TTFiZJTKZP",
  'UXP/USDC' : "7KQpsp914VYnh62yV6AGfoG9hprfA14SgzEyqr6u9NY1", 
  'VI/USDC' : "5fbYoaSBvAD8rW6zXo6oWqcCsgbYZCecbxAouk97p8SM",
  'WAG/USDC' : "BHqcTEDhCoZgvXcsSbwnTuzPdxv1HPs6Kz4AnPpNrGuq",
  'WOO/USDC' : "2Ux1EYeWsxywPKouRCNiALCZ1y3m563Tc4hq1kQganiq",
  'WOOF/USDC' : "CwK9brJ43MR4BJz2dwnDM7EXCNyHhGqCJDrAdsEts8n5",
  'XTAG/USDC' : "6QM3iZfkVc5Yyb5z8Uya1mvqU1JBN9ez81u9463px45A",
  'XVC/USDC' :"8GgBoTphoBifhwuybFguKevdjXTYGBLGcwpgG8cCewwk",
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
app.use(cors())

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
