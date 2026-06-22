**区块链元宇宙模块化实验平台建设方案**

**Metaverse Blockchain Modular Experiment Platform**

增强版：集成真实链接入、实验装配层、虚拟时钟、Trace 存储优化、CI/CD 与数据生命周期管理

版本：V0-V3 分阶段建设方案（增强修订版）

适用对象：课题组公共实验环境、论文实验平台、跨链与分片系统原型验证

日期：2026 年 6 月

# **修订说明**

本增强版在原有“链上可信 trace + 可控模拟负载 + 插件化协议实验 + 可视化管理”的总体定位上，进一步补充了真实链数据接入方式、实验装配层、虚拟时钟与离散事件模拟、Trace 存储格式优化、数据生命周期管理、CI/CD 防腐测试、状态机 Skip/Resume 以及课题组多人插件协作机制。修订目标不是推翻原方案，而是使其从架构蓝图进一步转化为可长期维护、可复现、可扩展的公共实验基础设施。

|                 |                                                  |        |
| --------------- | ------------------------------------------------ | ------ |
| **新增内容**        | **作用**                                           | **阶段** |
| 真实链接入流程         | 把实验负载转成链上可执行业务交易，再采集链上执行结果生成统一 trace。            | V1     |
| 实验装配层           | 让成员的新算法插件与已有共识、分片、执行、提交、跨链组件组合成完整系统。             | V0/V1  |
| 虚拟时钟 / DES      | 避免 time.Sleep 污染大规模实验的 P99 和吞吐结果。                | V0     |
| Trace 存储升级      | V0/V1 使用 jsonl.gz 流式读写，V2/V3 支持 Parquet / Arrow。 | V0-V3  |
| Skip/Resume 状态机 | 支持复用已有 trace，只重放不同执行策略或重新生成报告。                   | V0/V1  |
| CI/CD 防腐测试      | 防止后续插件或结构修改破坏已有 baseline。                        | V1     |
| 数据保留策略          | 避免共享服务器 trace、日志和中间结果长期堆积导致磁盘爆满。                 | V1/V2  |

# **一、平台总体定位**

本平台不是单纯的元宇宙展示 Demo，也不是只在本地运行的交易模拟器，而是面向区块链元宇宙场景的模块化实验平台。其核心目标是同时支持真实链上业务语义、可控模拟负载、异构链跨链交易、链内分片执行、插件化协议对比、实验装配、实时监控和结果复现。

平台的基本原则是：真实链层负责业务语义与执行真实性，模块化实验层负责可控性与可复现实验，实验装配层负责插件组合与合法性校验，Web 前端负责可用性与协作，工程化底座负责性能、数据生命周期和持续集成。

|        |                                                                          |
| ------ | ------------------------------------------------------------------------ |
| **目标** | **说明**                                                                   |
| 真实性    | 通过 Fabric / EVM / MockChain 部署元宇宙业务合约或链码，生成具有链上执行语义的 chain-backed trace。 |
| 可控性    | 支持 Zipf 偏斜、热点比例、跨片比例、跨链比例、突发强度、冲突注入、finality 延迟、委员会规模等参数配置。              |
| 扩展性    | 共识、共识分片、状态分片、执行分片、交易路由、跨片协议、跨链协议、执行协议、提交协议均以插件形式接入。                      |
| 可组合性   | 通过 Experiment Composer 将新算法插件与默认组件组合成完整实验系统，避免每个成员重复搭建底层环境。              |
| 可复现性   | 每次实验保存 seed、config.yaml、trace\_meta、插件版本、运行日志、指标文件和报告。                   |
| 可维护性   | 通过 CI/CD、sanity benchmark、数据保留策略和插件 schema 降低长期维护成本。                     |

# **二、总体架构**

平台采用“前端配置 + 后端控制 + 实验装配 + 链上 trace + 模块化重放 + 指标报告 + 工程化治理”的分层架构。原方案中的链上 trace 和模块化重放路径继续保留，但在 Backend Controller 与 Modular Experiment Engine 之间新增 Experiment Composer，用于把用户选择的新算法插件和已有默认组件装配成可运行系统。

> Web Frontend
> 
> ├─ 实验模板 / 插件选择 / 负载配置 / 拓扑设计 / 运行控制 / 结果对比
> 
> ↓ REST API / WebSocket
> 
> Backend Controller
> 
> ├─ 配置生成 / 参数校验 / 实验队列 / 任务调度 / 日志推送 / 数据保留
> 
> ↓
> 
> Experiment Composer
> 
> ├─ 插件注册 / 组件兼容性检查 / 默认组件补齐 / 完整 config.yaml 生成
> 
> ↓
> 
> Workload & Trace Layer
> 
> ├─ synthetic workload / chain-backed workload / hybrid workload / trace 转换
> 
> ↓
> 
> Chain Backend Layer
> 
> ├─ Fabric / EVM / MockChain / optional IPFS
> 
> ↓ trace.jsonl.gz / cross\_trace.jsonl.gz / trace\_meta.json
> 
> Modular Experiment Engine
> 
> ├─ 虚拟时钟 / 共识 / 分片 / 路由 / 跨片 / 跨链 / 执行 / 提交
> 
> ↓
> 
> Metrics & Report Layer
> 
> ├─ TPS / P99 / finality / pending pool / rollback / control overhead / 报告

重点原则是：真实链层不承载所有可变协议。真实链主要生成具有业务语义和链上执行结果的 trace；共识模型切换、分片策略对比、执行调度对比、跨链协议压力实验等主要在自研模块化实验引擎中完成。这样既避免重写真实链底层，又能保证论文实验的变量可控。

# **三、运行模式与适用边界**

为了避免把“真实上链”和“模块化模拟”混为一谈，平台需要明确区分三种运行模式。不同研究问题应选择不同模式，而不是所有算法都强行部署到真实链底层。

|                         |                               |                                |                  |
| ----------------------- | ----------------------------- | ------------------------------ | ---------------- |
| **运行模式**                | **主要用途**                      | **适合测试的内容**                    | **限制**           |
| Replay Mode             | 读取已有 trace，在模块化 executor 中重放。 | 共识模型、状态分片、路由、执行调度、提交协议、跨链窗口策略。 | 不直接反映真实链运行开销。    |
| Chain-backed Trace Mode | 将实验负载转为真实链交易，执行后采集统一 trace。   | 元宇宙业务语义、链上延迟、合约调用、真实事件采集。      | 底层共识和执行机制不宜频繁修改。 |
| Integrated Chain Mode   | 少数模块真实部署到链上或链下服务中。            | 合约逻辑、桥服务、事件监听、证书验证、链码功能。       | 工程复杂，适合作为后期增强。   |

一般情况下，课题组成员改进共识、分片、执行、提交或跨链调度算法时，优先使用 Chain-backed Trace Mode + Replay Mode：先由真实链或模拟器生成有业务语义的 trace，再在 executor 中做可控对比。只有合约逻辑、桥服务、链码功能和事件监听等模块才适合直接上链实现。

# **四、部署模式与服务器适配**

完整平台普通学校电脑很难稳定运行，因此需要设计成分层适配：普通电脑用于访问前端和本地小规模开发，正式实验放到课题组共享服务器；后期再扩展到多服务器分布式部署。

|                  |                                                                              |                                    |                                           |
| ---------------- | ---------------------------------------------------------------------------- | ---------------------------------- | ----------------------------------------- |
| **模式**           | **启动内容**                                                                     | **适用场景**                           | **推荐配置**                                  |
| Local Dev Mode   | 前端、后端、MockChain、小规模 synthetic workload、小型 executor。                          | 普通电脑开发、调试插件、跑 1000-10000 笔交易。      | 4-8 核 CPU / 16 GB 内存 / 100-200 GB SSD。    |
| Lab Server Mode  | 前端、后端、Fabric、workload generator、trace collector、executor、Prometheus、Grafana。 | 课题组共享实验、链上 trace、baseline 对比、论文实验。 | 16 核 64 GB 起步，推荐 32 核 128 GB / 2 TB NVMe。 |
| Distributed Mode | 多服务器 Fabric、多组织多 peer、多链跨链、独立监控节点。                                           | 后期大规模分布式实验和跨机器网络实验。                | 3-5 台服务器，每台 16-32 核 / 64 GB+。             |

服务器建议：第一版可先使用一台 16 核 / 64 GB / 1 TB NVMe 的共享服务器；若经费允许，优先选择 32 核 / 128 GB / 2 TB NVMe。GPU 不是必需项。需要特别注意的是，Lab Server Mode 必须启用数据保留策略，否则多人频繁实验会快速消耗磁盘空间。

# **五、真实链数据接入方式**

实验数据接入真实链时，不应把已有 trace 原封不动上传到链上，而应把实验负载转成真实链可以执行的业务交易调用，然后从链上执行结果反向生成统一 trace。这样既保留链上语义，又能继续支撑模块化重放和公平对比。

> 实验数据 / synthetic workload / 元宇宙业务数据
> 
> ↓ Data Adapter
> 
> 链码 / 智能合约交易调用
> 
> ↓ Fabric / EVM / MockChain 执行
> 
> 区块事件 / 交易回执 / 链上延迟 / 读写集合
> 
> ↓ Trace Collector
> 
> 统一 trace.jsonl.gz / cross\_trace.jsonl.gz / trace\_meta.json
> 
> ↓ Modular Executor Replay
> 
> 协议对比 / baseline 对比 / 消融实验

|                           |                   |                                             |                                               |
| ------------------------- | ----------------- | ------------------------------------------- | --------------------------------------------- |
| **实验交易类型**                | **链上合约/链码**       | **链上函数**                                    | **主要状态键**                                     |
| asset\_transfer           | AssetContract     | TransferAsset(asset\_id, from, to)          | asset:{id}, owner:{user}                      |
| asset\_trade              | AssetContract     | TradeAsset(asset\_id, seller, buyer, price) | asset:{id}, balance:{seller}, balance:{buyer} |
| scene\_join               | SceneContract     | JoinScene(user\_id, scene\_id)              | scene:{id}, avatar:{user}                     |
| inventory\_update         | InventoryContract | UpdateInventory(user\_id, item\_id, delta)  | inventory:{user}                              |
| reward\_claim             | RewardContract    | ClaimReward(user\_id, pool\_id, amount)     | reward\_pool:{id}, balance:{user}             |
| cross\_chain\_asset\_move | BridgeContract    | LockAsset(asset\_id, target\_chain)         | asset:{id}, bridge\_lock:{id}                 |

第一版建议优先接 Fabric。Fabric 更适合课题组私有部署，不需要真实代币，交易确认和链码逻辑较容易控制。EVM 可作为辅助环境，用于 Solidity 合约、事件监听和跨链桥合约原型。

## **5.1 access\_schema.yaml**

由于真实链回执不一定直接提供完整访问集合，平台需要为链码或合约函数提供 access\_schema.yaml。第一版可先用 schema 生成 read\_set、write\_set、access\_list 和 commutative 标记；后续再补充 Fabric RWSet 解析或 EVM debug trace 作为校验。

> contract: asset
> 
> functions:
> 
> TransferAsset:
> 
> read\_set:
> 
> \- "asset:{asset\_id}"
> 
> write\_set:
> 
> \- "asset:{asset\_id}"
> 
> commutative: false
> 
> AddReward:
> 
> read\_set:
> 
> \- "reward\_pool:{pool\_id}"
> 
> write\_set:
> 
> \- "reward\_pool:{pool\_id}"
> 
> \- "balance:{user\_id}"
> 
> commutative: true
> 
> update\_type: delta

# **六、平台一级模块划分**

|                        |                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------- |
| **一级模块**               | **主要职责**                                                                          |
| chain\_backend         | Fabric / EVM / MockChain / optional IPFS，负责链上执行环境和 trace 来源。                      |
| workload               | 真实负载、链上生成负载、模拟负载、混合负载。                                                            |
| workload\_profile      | 从真实或 chain-backed trace 中提取交易比例、访问集合大小、热点分布、到达过程等参数。                              |
| trace                  | 单链 trace、跨片 trace、跨链 multi-stage trace 的采集、转换、校验、压缩和写入。                           |
| experiment\_composer   | 读取插件声明，检查兼容性，自动补齐默认组件，生成完整可运行 config。                                             |
| consensus\_protocol    | Raft-like、PBFT-like、HotStuff-like、PoW-like、PoS-like、DAG-like 等共识模型或成本模型。          |
| consensus\_sharding    | 单共识组、多共识组、beacon+shards、committee-based 等共识分片模型。                                  |
| state\_sharding        | 状态键到状态分片的持久映射 phi。                                                                |
| execution\_sharding    | 交易到执行分片的映射 psi\_t。                                                                |
| routing                | 批次级状态键到执行侧位置的路由 M\_t。                                                             |
| cross\_shard\_protocol | 链内跨片协议，例如 lock-based、2PC-like、receipt-based、optimistic。                           |
| cross\_chain\_protocol | 链间跨链协议，例如 committee bridge、light-client relay、lock-mint、message passing、MetaFlow。 |
| execution              | serial、optimistic、conservative、Block-STM-like、Calvin-like、dual-track。             |
| commit                 | normal commit、validate commit、2PC-like commit、hot update aggregation。             |
| virtual\_time          | 虚拟时钟、离散事件队列、网络延迟和 finality 延迟的逻辑推进。                                               |
| network\_and\_fault    | 网络延迟、带宽、抖动、慢分片、失败分片、恶意委员会、热点攻击、依赖注入。                                              |
| metrics                | TPS、P99、跨片、跨链、finality、pending pool、回滚、锁争用、控制开销等指标。                               |
| governance             | CI/CD、数据保留策略、实验归档、插件版本记录和健康检查。                                                    |

# **七、实验装配层与插件组合机制**

课题组成员在做算法改进时，通常只改进某一类模块，例如共识算法、分片算法、执行算法或跨链协议。但单独测试一个算法没有完整系统上下文，无法得到可比较的 TPS、P99、finality、rollback 或 pending pool 指标。因此平台必须提供 Experiment Composer，将新算法插件与已有默认组件组合成完整可运行系统。

> 新算法插件
> 
> ↓
> 
> Plugin Registry 读取插件能力和依赖
> 
> ↓
> 
> Experiment Composer 检查兼容性并补齐默认组件
> 
> ↓
> 
> 生成完整 config.yaml
> 
> ↓
> 
> Backend Controller 启动实验
> 
> ↓
> 
> Executor 加载模块并输出统一指标

## **7.1 V0 最基础插件包**

V0 阶段的插件选择原则是“保守、完整、可验证”。每类核心模块只实现一个默认插件，目的不是追求性能或创新，而是先跑通前端配置、后端装配、负载生成、Trace 读写、Executor 重放、指标输出和结果展示的完整链路。

|                        |                                    |                                                                                               |
| ---------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------- |
| **插件类型**               | **V0 默认插件**                        | **作用/说明**                                                                                     |
| chain\_backend         | mockchain                          | 模拟单链执行和确认，不依赖 Fabric / EVM，便于本地开发和链路验证。                                                       |
| workload               | asset\_hotspot                     | 生成资产热点交易，覆盖元宇宙资产访问与热点状态场景。                                                                    |
| trace                  | jsonl\_gzip                        | 正式实验使用 trace.jsonl.gz 流式读写，小规模调试可保留明文 trace.jsonl。                                            |
| consensus\_protocol    | simple\_ordering                   | 按 timestamp 或 tx\_id 排序并按 block\_size 打包，模拟 block interval 和 finality delay。                  |
| consensus\_sharding    | single\_group                      | 全部交易进入一个共识组，避免 V0 引入多委员会或 beacon 复杂度。                                                         |
| state\_sharding        | hash\_state\_sharding              | 实现 phi: state\_key -\> state\_shard，作为最基础状态持久映射。                                              |
| execution\_sharding    | hash\_execution\_sharding          | 根据 primary\_key 或 tx\_id 将交易分配到执行分片。                                                          |
| routing                | hash\_routing                      | 实现 M\_t(key)=hash(key) mod execution\_shard\_count，作为默认路由 baseline。                           |
| cross\_shard\_protocol | local\_only                        | V0 先统计跨片比例和远程访问，不实现完整 2PC；跨片协调放到后续阶段。                                                         |
| cross\_chain\_protocol | disabled                           | V0 先跑单链闭环，不启用跨链协议。                                                                            |
| execution              | serial\_execution                  | 每个执行分片内部串行执行，结果最容易验证。                                                                         |
| commit                 | normal\_commit                     | 执行成功直接提交写集，失败则记录 abort，不做热点聚合或验证提交。                                                           |
| runtime                | virtual\_clock                     | 所有网络延迟、远程访问和 finality 等待均用虚拟时间推进，避免 time.Sleep 污染指标。                                          |
| network\_model         | fixed\_latency\_model              | 使用固定远程访问延迟和区块确认延迟，便于结果复现。                                                                     |
| metrics                | basic\_metrics                     | 输出 tx\_count、success\_count、TPS、avg latency、P95、P99、cross\_shard\_ratio、remote\_fetch\_count。 |
| experiment\_composer   | default\_composer                  | 根据默认单链模板自动补齐所有组件，使用户只选少量参数也能运行完整实验。                                                           |
| frontend\_template     | default\_single\_chain\_experiment | 提供基础前端实验模板，支持创建实验、查看组件组合、运行日志和结果。                                                             |

## **7.2 实验模板**

|                          |                                                                |                                                                    |
| ------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------------ |
| **模板**                   | **固定组件**                                                       | **允许替换组件**                                                         |
| consensus\_comparison    | workload、state\_sharding、execution、commit、metrics              | consensus\_protocol、consensus\_sharding                            |
| sharding\_comparison     | workload、consensus、execution、commit                            | state\_sharding、execution\_sharding、routing、cross\_shard\_protocol |
| execution\_comparison    | workload、consensus、sharding、routing、commit                     | execution                                                          |
| commit\_comparison       | workload、consensus、sharding、routing、execution                  | commit                                                             |
| cross\_chain\_comparison | source\_chain、target\_chain、committee、finality\_model、workload | cross\_chain\_protocol、batch\_policy、window\_policy                |
| metaflow\_comparison     | 异构链拓扑、committee、pending pool、timeout policy                    | MetaFlow 的 B / D 调节策略和 baseline 跨链协议                               |

## **7.3 插件声明文件**

每个插件除了代码实现外，还必须提供 plugin.yaml，用于声明插件类型、输入输出、依赖条件、兼容组件、禁用组合、默认参数和指标。前端据此生成表单，后端据此进行强校验，CI 据此运行 sanity benchmark。

> name: my\_adaptive\_sharding
> 
> type: state\_sharding
> 
> version: 0.1.0
> 
> maturity: experimental
> 
> compatible\_with:
> 
> execution\_sharding:
> 
> \- static
> 
> \- dynamic
> 
> routing:
> 
> \- hash
> 
> \- coaccess
> 
> requires:
> 
> trace\_fields:
> 
> \- access\_list
> 
> \- read\_set
> 
> \- write\_set
> 
> incompatible\_with:
> 
> \- dynamic\_state\_migration
> 
> metrics:
> 
> \- cross\_shard\_ratio
> 
> \- remote\_fetch\_count
> 
> \- state\_imbalance
> 
> \- control\_overhead\_ms

# **八、链内分片与协议抽象**

分片不能只设计成一个笼统模块，而应拆成共识分片、状态分片、执行分片、交易路由和链内跨片协议。尤其要区分 phi、M\_t 和 psi\_t，避免让系统口径混淆为在线状态迁移或动态重分片。

|        |                                       |                           |
| ------ | ------------------------------------- | ------------------------- |
| **概念** | **映射/作用**                             | **说明**                    |
| 状态分片   | phi: state\_key -\> state\_shard      | 表示状态键的持久存储位置，通常固定或慢变。     |
| 执行路由   | M\_t: state\_key -\> execution\_shard | 表示批次级执行侧目标位置，不改变底层持久状态位置。 |
| 交易执行分片 | psi\_t: tx -\> execution\_shard       | 表示交易实际被送到哪个执行分片处理。        |
| 共识分片   | tx/block -\> consensus\_group         | 表示不同交易或区块由哪个共识组排序和确认。     |
| 链内跨片协议 | multi-shard tx -\> commit decision    | 负责同一条链内部多分片交易的协调提交。       |

论文口径提醒：phi 是状态持久位置；M\_t 是批次级执行路由；psi\_t 是交易执行分片。M\_t 不改变底层持久状态位置，因此不应写成状态迁移或动态重分片机制。

# **九、元宇宙业务模型**

为了避免平台退化为普通区块链压测工具，元宇宙业务对象需要固定为若干可复用状态模型，并围绕这些对象生成负载、合约和 trace。

|             |                                                            |                                                              |
| ----------- | ---------------------------------------------------------- | ------------------------------------------------------------ |
| **业务对象**    | **状态示例**                                                   | **典型交易**                                                     |
| Asset       | asset:{id}, owner:{user}, metadata:{id}                    | mint\_asset、asset\_transfer、asset\_trade、lock\_asset         |
| Avatar      | avatar:{user}, avatar\_state:{user}                        | create\_avatar、update\_avatar、scene\_authorize               |
| Scene       | scene:{id}, scene\_load:{id}, scene\_member:{scene}:{user} | scene\_join、scene\_leave、scene\_interact                     |
| Inventory   | inventory:{user}, item:{id}                                | inventory\_update、equip\_item、consume\_item                  |
| Reward      | reward\_pool:{id}, balance:{user}                          | reward\_claim、add\_reward、batch\_reward                      |
| Marketplace | order:{id}, asset:{id}, balance:{user}                     | create\_order、match\_order、settle\_trade                     |
| Bridge      | bridge\_lock:{id}, wrapped\_asset:{id}, cert:{id}          | cross\_chain\_asset\_move、mint\_wrapped\_asset、refund\_asset |

# **十、元宇宙跨链实验支持**

平台需要支持多链元宇宙场景，尤其是异构链不同处理速度、不同 finality 延迟、不同吞吐上限，以及基于共识管委会的跨链桥实验。该部分可直接支撑 MetaFlow 的异构链跨链协议实验。

|                                 |                               |
| ------------------------------- | ----------------------------- |
| **跨链场景**                        | **说明**                        |
| AssetChain -\> SceneChain       | 资产链上的 NFT 或虚拟资产进入场景链使用。       |
| SceneChain -\> RewardChain      | 用户完成场景事件后，奖励链发放积分或代币。         |
| MarketplaceChain -\> AssetChain | 市场链完成撮合后，资产链更新所有权。            |
| IdentityChain -\> SceneChain    | 身份链验证 Avatar / DID 后，场景链授权进入。 |
| 多场景链迁移                          | 用户在多个元宇宙平台或场景链之间迁移资产和状态。      |

## **10.1 共识管委会跨链桥流程**

> 用户发起跨链交易
> 
> ↓
> 
> 源链执行 Lock / Burn / Escrow / Event
> 
> ↓
> 
> 等待源链 finality
> 
> ↓
> 
> 管委会监听源链事件并达成证书共识
> 
> ↓
> 
> 生成 MintCert / UpdateCert / RewardCert
> 
> ↓
> 
> 提交到目标链
> 
> ↓
> 
> 目标链验证证书并执行 Mint / Unlock / Update
> 
> ↓
> 
> 等待目标链 finality
> 
> ↓
> 
> 跨链交易完成 / 超时 / 退款

## **10.2 Pending Pool 状态**

|                 |                                 |
| --------------- | ------------------------------- |
| **状态**          | **含义**                          |
| Created         | 跨链交易已创建。                        |
| SourceExecuted  | 源链动作已执行。                        |
| SourceFinalized | 源链已达到 finality。                 |
| CertGenerated   | 管委会证书已生成。                       |
| WaitingMint     | 等待目标链执行 mint / unlock / update。 |
| WaitingFinality | 目标链已执行，等待目标链 finality。          |
| Completed       | 跨链交易完成。                         |
| Timeout         | 等待超过超时边界。                       |
| Refunded        | 已触发退款或补偿。                       |
| Failed          | 跨链交易失败。                         |

跨链实验重点指标包括：端到端跨链延迟、source finality wait、target finality wait、committee cert delay、pending pool size、WaitingMint、WaitingFinality、timeout rate、refund rate、completed rate。

# **十一、负载与 Trace 设计**

## **11.1 负载来源**

|                       |                                             |              |             |
| --------------------- | ------------------------------------------- | ------------ | ----------- |
| **负载类型**              | **作用**                                      | **优点**       | **限制**      |
| real workload         | 接入真实链上历史负载或已有应用 trace。                      | 真实性强。        | 变量控制困难。     |
| chain-backed workload | 在 Fabric / EVM 上部署元宇宙合约并生成交易。               | 有链上语义且可控。    | 仍需人工设计业务场景。 |
| synthetic workload    | 完全由生成器构造负载。                                 | 适合压力实验和参数扫描。 | 单独使用可信度弱。   |
| hybrid workload       | 从真实或 chain-backed trace 学分布，再放大或注入热点/跨片/跨链。 | 兼顾真实性和可控性。   | 实现复杂度较高。    |

## **11.2 关键负载参数**

|                                  |                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------- |
| **参数**                           | **含义**                                                                        |
| tx\_count                        | 交易总数。                                                                         |
| tx\_mix                          | 不同交易类型比例，例如 asset\_trade、scene\_join、reward\_claim、cross\_chain\_asset\_move。 |
| arrival\_rate / burst\_rate      | 平稳到达率和突发到达率。                                                                  |
| zipf\_theta                      | 状态访问偏斜程度。                                                                     |
| hot\_key\_ratio / hot\_tx\_ratio | 热点键比例和访问热点的交易比例。                                                              |
| read\_write\_ratio               | 读写比例。                                                                         |
| access\_set\_size                | 访问集合大小。                                                                       |
| commutative\_update\_ratio       | 可交换增量更新比例，对热点更新聚合很关键。                                                         |
| cross\_shard\_ratio              | 跨片交易比例。                                                                       |
| cross\_chain\_ratio              | 跨链交易比例。                                                                       |
| conflict\_injection\_ratio       | 依赖和冲突注入比例。                                                                    |
| finality\_delay\_ms              | 源链或目标链 finality 延迟。                                                           |
| committee\_size / threshold      | 跨链管委会规模和证书阈值。                                                                 |

## **11.3 Trace 输出字段**

平台统一使用 trace.jsonl.gz、cross\_trace.jsonl.gz 和 trace\_meta.json 作为正式实验入口；小规模调试可保留 trace.jsonl 明文样例。

> 单链 trace 字段：
> 
> tx\_id, tx\_type, timestamp, chain\_id, contract, function, args,
> 
> read\_set, write\_set, access\_list, commutative, update\_type,
> 
> status, chain\_latency\_ms
> 
> 跨链 trace 字段：
> 
> cross\_tx\_id, scenario, protocol, source\_chain, target\_chain,
> 
> stages\[\], end\_to\_end\_latency\_ms, status
> 
> trace\_meta 字段：
> 
> actual\_tx\_mix, actual\_zipf\_theta, actual\_hot\_key\_ratio,
> 
> actual\_cross\_shard\_ratio, actual\_cross\_chain\_ratio,
> 
> avg\_read\_set\_size, avg\_write\_set\_size, conflict\_ratio, seed,
> 
> trace\_format, compression, schema\_version

## **11.4 Trace 存储格式优化**

|             |                          |                                         |
| ----------- | ------------------------ | --------------------------------------- |
| **阶段**      | **格式**                   | **说明**                                  |
| V0/V1 调试    | trace.jsonl              | 小规模样例保留明文格式，便于阅读和教学。                    |
| V0/V1 正式实验  | trace.jsonl.gz           | Python 边生成边压缩，Go 边解压边重放，降低磁盘 I/O 和存储开销。 |
| V2/V3 归档与分析 | Apache Parquet / Arrow   | 用于大规模 trace 的列式存储、批量分析和长期归档。            |
| 大规模实验       | trace\_part\_\*.jsonl.gz | 采用分片文件，便于并行读取、断点恢复和生命周期管理。              |

# **十二、Executor 底层：虚拟时钟与离散事件模拟**

模块化 executor 不能使用 time.Sleep 来模拟网络延迟、远程状态拉取和 finality 等待。真实 sleep 会使百万级实验耗时过长，并且 Go runtime 调度、线程唤醒、系统负载和 GC 会污染 P99 尾延迟。因此 executor 必须支持虚拟时钟或离散事件模拟。

|               |                                           |                         |
| ------------- | ----------------------------------------- | ----------------------- |
| **时间模式**      | **适用场景**                                  | **说明**                  |
| virtual\_time | Replay Mode、协议模拟、网络延迟、finality 延迟、远程状态等待。 | 通过事件时间戳推进逻辑时间，不让线程真实挂起。 |
| wall\_clock   | Fabric / EVM 真实链压测、工程运行耗时统计。              | 使用真实物理时间衡量实际链执行和系统资源开销。 |

> executor/time/
> 
> ├── clock.go
> 
> ├── virtual\_clock.go
> 
> ├── wall\_clock.go
> 
> ├── event\_queue.go
> 
> └── scheduler.go
> 
> 配置示例：
> 
> runtime:
> 
> clock\_mode: virtual
> 
> event\_simulation: true
> 
> remote\_fetch\_latency\_ms: 5
> 
> finality\_delay\_ms: 1000

在 virtual\_time 模式下，远程状态访问不再执行 time.Sleep(5ms)，而是计算 remote\_fetch\_done\_time = tx\_start\_time + remote\_fetch\_latency。交易延迟统计为 commit\_done\_time - tx\_arrival\_time。这样能够用较短的物理运行时间得到稳定的逻辑延迟指标。

# **十三、Web 前端设计**

前端不是展示型页面，而是实验配置中心、插件选择中心、实验装配入口、实验运行控制台、实时监控面板和结果对比平台。第一版不应追求完整大屏，而应优先做 Experiments、Run Console、Results 三个闭环页面。

|                |                                                    |
| -------------- | -------------------------------------------------- |
| **页面**         | **主要功能**                                           |
| Overview       | 平台总览、当前任务、服务器资源、最近结果、告警信息。                         |
| Experiments    | 新建实验、选择实验模板、选择插件、生成完整 config、查看实验详情。               |
| Workloads      | 真实负载、链上负载、模拟负载、混合负载配置。                             |
| Topology       | 链数量、分片拓扑、跨链连接关系、异构链配置。                             |
| Protocols      | 共识、共识分片、状态分片、跨片、跨链、执行、提交插件选择。                      |
| Composer       | 显示插件兼容性、默认组件补齐结果和非法组合提示。                           |
| Run Console    | 实验运行进度、实时日志、失败提示、启动/停止控制。                          |
| Live Monitor   | TPS、P99、CPU、内存、网络、pending pool、finality wait 实时监控。 |
| Results        | 多 baseline 结果对比、图表导出、CSV / LaTeX / Markdown 报告导出。  |
| Trace Explorer | 交易类型分布、热点 key、访问集合大小、跨片/跨链统计。                      |
| Admin          | 用户、权限、服务器、插件版本、数据清理和实验归档。                          |

前端原则：前端不要直接操作 Docker、Fabric 或实验进程，而是通过 Backend Controller 调用实验控制服务。这样更安全、可记录、可恢复，也便于多人使用。

# **十四、后端控制器、任务状态机与 Skip/Resume**

后端负责配置生成、参数校验、插件组合检查、实验队列、任务调度、日志推送、结果管理和数据生命周期管理。增强版需要支持完整流程和跳跃式流程两种状态机模式。

> 完整流程：
> 
> CREATED
> 
> ↓ CONFIG\_VALIDATED
> 
> WORKLOAD\_GENERATING
> 
> ↓ CHAIN\_STARTING
> 
> CHAINCODE\_DEPLOYING
> 
> ↓ CHAIN\_WORKLOAD\_RUNNING
> 
> TRACE\_COLLECTING
> 
> ↓ TRACE\_READY
> 
> REPLAY\_RUNNING
> 
> ↓ METRICS\_COLLECTING
> 
> REPORT\_GENERATING
> 
> ↓
> 
> COMPLETED / FAILED / CANCELLED
> 
> 跳跃/复用流程：
> 
> TRACE\_READY -\> REPLAY\_RUNNING -\> METRICS\_COLLECTING -\> REPORT\_GENERATING
> 
> REPLAY\_FINISHED -\> REPORT\_GENERATING

|                |                                          |                                               |
| -------------- | ---------------------------------------- | --------------------------------------------- |
| **模式**         | **作用**                                   | **前置校验**                                      |
| full\_pipeline | 从负载生成、链启动、链上执行、trace 采集到 replay 和报告全部执行。 | 检查链环境、插件组合、负载配置、输出目录。                         |
| trace\_only    | 只生成或采集 trace，不运行 executor。               | 检查 workload、chain backend、schema。             |
| replay\_only   | 复用已有 trace，只对比不同执行策略或协议插件。               | 检查 trace 文件、trace\_meta、schema\_version、插件版本。 |
| report\_only   | 复用已有 summary 和 metrics，仅重新生成报告和图表。       | 检查 metrics 文件、报告模板。                           |
| resume\_from   | 从失败或指定阶段恢复运行。                            | 检查阶段产物是否完整、config 是否匹配。                       |

## **14.1 后端服务**

|                      |                                             |
| -------------------- | ------------------------------------------- |
| **后端服务**             | **职责**                                      |
| ExperimentController | 完整实验流程控制。                                   |
| ConfigGenerator      | 前端表单转 config.yaml。                          |
| ConfigValidator      | 检查配置格式和插件组合合法性。                             |
| ExperimentComposer   | 根据模板、插件声明和默认组件生成完整系统配置。                     |
| CompatibilityChecker | 检查插件输入输出、依赖、互斥关系和运行模式是否匹配。                  |
| TaskQueue            | 管理实验队列、并发限制、大/小实验分类。                        |
| ProcessRunner        | 运行外部脚本和 Go executor，捕获 stdout/stderr 并推送日志。 |
| ProtocolRegistry     | 读取插件定义，返回前端可选项。                             |
| TraceService         | 管理 trace 采集、校验、转换、压缩和下载。                    |
| MetricsService       | 读取 CSV、Prometheus 指标和实验 summary。            |
| ResultService        | 保存 summary、生成报告、导出图表和表格。                    |
| RetentionService     | 按实验类型执行删除、压缩、归档和 pinned 保护策略。               |
| WebSocketManager     | 实时推送日志、状态和指标。                               |

# **十五、仓库顶层目录结构**

> metaverse-chainlab/
> 
> ├── README.md
> 
> ├── Makefile
> 
> ├── .env.example
> 
> ├── docker-compose.yml
> 
> ├── docker-compose.dev.yml
> 
> ├── docker-compose.server.yml
> 
> ├── docker-compose.monitor.yml
> 
> ├── configs/
> 
> ├── frontend/
> 
> ├── backend/
> 
> ├── chain/
> 
> ├── workload/
> 
> ├── trace/
> 
> ├── protocols/
> 
> ├── executor/
> 
> ├── experiments/
> 
> ├── metrics/
> 
> ├── deploy/
> 
> ├── data/
> 
> ├── scripts/
> 
> ├── tests/
> 
> ├── .github/
> 
> └── docs/

|              |                                                                                   |
| ------------ | --------------------------------------------------------------------------------- |
| **目录/文件**    | **应包含内容**                                                                         |
| README.md    | 平台定位、快速启动、运行模式、目录说明。                                                              |
| Makefile     | 常用命令封装，例如 up-dev、up-server、fabric-up、generate-workload、run-experiment、run-sanity。 |
| configs/     | 实验配置、负载配置、链配置、插件配置、模板和 schema。                                                    |
| frontend/    | React 前端。                                                                         |
| backend/     | FastAPI 后端控制器、实验装配层、数据保留服务。                                                       |
| chain/       | Fabric、EVM、MockChain 链环境。                                                         |
| workload/    | 负载生成、真实 trace 加载、混合负载校准、chain-backed 交易调用。                                        |
| trace/       | trace schema、采集、转换、校验、压缩和写入。                                                      |
| protocols/   | 共识、共识分片、跨片、跨链协议插件及声明文件。                                                           |
| executor/    | Go 模块化实验执行器和虚拟时钟。                                                                 |
| experiments/ | 实验运行、批量扫描、报告生成。                                                                   |
| metrics/     | Prometheus、Grafana、指标收集和报告生成。                                                     |
| data/        | 实验产生的 trace、结果、日志和报告。                                                             |
| tests/       | 单元测试、集成测试、sanity benchmark、golden trace。                                          |
| .github/     | GitHub Actions CI/CD 工作流。                                                         |
| docs/        | 架构、安装、插件开发、实验复现、数据保留和故障排查文档。                                                      |

# **十六、configs 目录设计**

> configs/
> 
> ├── experiments/
> 
> │ ├── v0\_mock\_asset.yaml
> 
> │ ├── v1\_asset\_hotspot\_ours.yaml
> 
> │ ├── v1\_baseline\_hash\_serial.yaml
> 
> │ ├── v1\_baseline\_blockstm\_like.yaml
> 
> │ ├── v1\_baseline\_calvin\_like.yaml
> 
> │ ├── v2\_cross\_chain\_committee.yaml
> 
> │ └── v2\_metaflow\_bridge.yaml
> 
> ├── templates/
> 
> │ ├── consensus\_comparison.yaml
> 
> │ ├── sharding\_comparison.yaml
> 
> │ ├── execution\_comparison.yaml
> 
> │ ├── commit\_comparison.yaml
> 
> │ └── cross\_chain\_comparison.yaml
> 
> ├── workloads/
> 
> ├── chains/
> 
> ├── plugins/
> 
> └── schemas/

configs 中新增 templates，用于定义不同研究方向的可替换模块和固定组件。这样成员不需要手动拼完整系统，而是在模板基础上替换自己的插件。

V0 默认实验配置示例：

experiment:

name: v0\_default\_asset\_hotspot

seed: 42

output\_dir: data/results/v0\_default\_asset\_hotspot

pipeline:

mode: full\_pipeline

template: default\_single\_chain\_experiment

runtime:

clock\_mode: virtual

event\_simulation: true

remote\_fetch\_latency\_ms: 5

finality\_delay\_ms: 100

chain\_backend:

plugin: mockchain

workload:

plugin: asset\_hotspot

tx\_count: 10000

zipf\_theta: 1.2

hot\_key\_ratio: 0.05

cross\_shard\_ratio: 0.2

trace:

format: jsonl\_gzip

output: trace.jsonl.gz

consensus\_protocol:

plugin: simple\_ordering

block\_size: 500

block\_interval\_ms: 100

finality\_delay\_ms: 100

consensus\_sharding:

plugin: single\_group

state\_sharding:

plugin: hash\_state\_sharding

shard\_count: 4

execution\_sharding:

plugin: hash\_execution\_sharding

shard\_count: 4

routing:

plugin: hash\_routing

cross\_shard\_protocol:

plugin: local\_only

cross\_chain\_protocol:

plugin: disabled

execution:

plugin: serial\_execution

commit:

plugin: normal\_commit

metrics:

plugin: basic\_metrics

output:

\- summary.csv

\- latency.csv

\- runtime.log

# **十七、frontend 目录设计**

> frontend/src/
> 
> ├── api/
> 
> ├── pages/
> 
> │ ├── Overview/
> 
> │ ├── Experiments/
> 
> │ ├── Composer/
> 
> │ ├── Workloads/
> 
> │ ├── Topology/
> 
> │ ├── Protocols/
> 
> │ ├── RunConsole/
> 
> │ ├── LiveMonitor/
> 
> │ ├── Results/
> 
> │ ├── TraceExplorer/
> 
> │ └── Admin/
> 
> ├── components/
> 
> ├── types/
> 
> ├── store/
> 
> └── utils/

|                                          |                                                               |
| ---------------------------------------- | ------------------------------------------------------------- |
| **前端文件/目录**                              | **应写模块**                                                      |
| api/client.ts                            | 统一封装后端 REST API 请求。                                           |
| api/websocket.ts                         | 连接 WebSocket，接收日志、状态和实时指标。                                    |
| pages/Experiments/ExperimentWizard.tsx   | 分步骤配置实验：基本信息、模板、链拓扑、负载、协议、指标、确认。                              |
| pages/Composer/ComposerPreview.tsx       | 展示自动补齐后的完整组件组合、兼容性检查结果和非法组合原因。                                |
| pages/Workloads/SyntheticConfig.tsx      | 配置 tx\_count、Zipf、热点比例、跨片比例、跨链比例、冲突注入。                        |
| pages/Topology/ChainTopologyDesigner.tsx | 配置链数量、每条链的共识、finality、吞吐、分片和跨链边。                              |
| pages/Protocols/ProtocolRegistry.tsx     | 展示所有插件、默认参数、依赖条件、成熟度和是否实现。                                    |
| pages/RunConsole/RuntimeLog.tsx          | 实时显示实验日志。                                                     |
| pages/LiveMonitor/PendingPoolPanel.tsx   | 展示跨链 pending pool、WaitingMint、WaitingFinality、timeout、refund。 |
| pages/Results/ResultCompare.tsx          | 多 baseline 指标对比。                                              |
| pages/TraceExplorer/HotKeyTable.tsx      | 展示热点 key 排名和访问次数。                                             |
| components/PluginSelector/               | 通用插件选择表单，根据 plugin schema 自动生成参数输入。                           |
| utils/configValidator.ts                 | 前端初步检查非法组合，后端仍需再次校验。                                          |

# **十八、backend 目录设计**

> backend/app/
> 
> ├── main.py
> 
> ├── settings.py
> 
> ├── api/
> 
> ├── core/
> 
> ├── services/
> 
> ├── db/
> 
> └── utils/

|                                    |                                                      |
| ---------------------------------- | ---------------------------------------------------- |
| **后端文件/目录**                        | **应写模块**                                             |
| main.py                            | FastAPI 入口，注册 router，初始化协议注册表、实验装配器和数据目录。            |
| settings.py                        | 统一配置：端口、数据目录、数据库、Fabric 路径、并发限制、retention 默认策略。      |
| api/experiments.py                 | 实验 CRUD、启动、停止、状态、日志、配置下载接口。                          |
| api/composer.py                    | 实验模板、插件组合预览、兼容性检查接口。                                 |
| api/workloads.py                   | 负载模板、生成负载、上传真实 trace、下载 trace 接口。                    |
| api/protocols.py                   | 插件列表、插件详情、插件组合校验接口。                                  |
| api/storage.py                     | 实验数据占用统计、归档、删除、pinned 标记接口。                          |
| core/experiment\_state.py          | 实验状态机枚举，支持 full\_pipeline、replay\_only、report\_only。 |
| services/experiment\_controller.py | 完整实验流程控制：生成负载、启动链、采集 trace、replay、报告。                |
| services/experiment\_composer.py   | 读取模板和插件声明，生成完整 config。                               |
| services/compatibility\_checker.py | 检查插件兼容性、输入输出、运行模式和互斥关系。                              |
| services/retention\_service.py     | 执行数据生命周期管理、压缩、归档和删除。                                 |
| services/result\_service.py        | 保存 summary、生成报告、导出图表和表格。                             |

# **十九、chain 目录设计**

> chain/
> 
> ├── fabric/
> 
> │ ├── network/
> 
> │ ├── chaincode/
> 
> │ │ ├── asset/
> 
> │ │ ├── scene/
> 
> │ │ ├── avatar/
> 
> │ │ ├── inventory/
> 
> │ │ └── reward/
> 
> │ ├── clients/
> 
> │ ├── caliper/
> 
> │ └── scripts/
> 
> ├── evm/
> 
> └── mockchain/

|                                            |                                                                                  |
| ------------------------------------------ | -------------------------------------------------------------------------------- |
| **文件/目录**                                  | **内容**                                                                           |
| fabric/network/                            | Fabric 网络脚本、组织、channel、docker 配置。                                                |
| fabric/chaincode/asset/chaincode.go        | MintAsset、TransferAsset、TradeAsset、LockAsset、UnlockAsset、MintWrappedAsset 等链码函数。 |
| fabric/chaincode/asset/model.go            | Asset、Owner、Balance、LockedStatus、WrappedAsset 等数据结构。                             |
| fabric/chaincode/asset/access\_schema.yaml | 每个链码函数对应的 read\_set、write\_set、access\_list、commutative 标记。                      |
| fabric/clients/fabric\_client.py           | 提交交易、查询状态、返回 tx\_id、latency、status。                                              |
| fabric/clients/event\_listener.py          | 监听 LockEvent、MintEvent、RewardEvent，供跨链桥服务使用。                                     |
| evm/contracts/                             | Solidity 资产、场景、奖励、桥合约，作为辅助 EVM 环境。                                               |
| mockchain/chain.py                         | 轻量链模拟器，用于本地小规模实验。                                                                |
| mockchain/finality.py                      | 确定性 finality、概率 finality、固定/可变 finality delay。                                   |

# **二十、workload 目录设计**

> workload/
> 
> ├── common/
> 
> ├── real\_loader/
> 
> ├── chain\_backed\_generator/
> 
> ├── synthetic\_generator/
> 
> ├── hybrid\_generator/
> 
> └── profiler/

|                                                        |                                                                                            |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| **文件/目录**                                              | **应写模块**                                                                                   |
| common/tx\_types.py                                    | 定义 asset\_transfer、asset\_trade、scene\_join、reward\_claim、cross\_chain\_asset\_move 等交易类型。 |
| common/key\_model.py                                   | 定义状态键格式，例如 asset:{id}、scene:{id}、reward\_pool:{id}。                                        |
| common/distributions.py                                | Zipf、均匀、泊松、突发、热点采样、跨片比例控制。                                                                 |
| common/access\_builder.py                              | 根据交易类型生成 read\_set、write\_set、access\_list、commutative flag。                               |
| real\_loader/                                          | 解析真实链上 trace、NFT 交易日志、Fabric/EVM 日志。                                                       |
| chain\_backed\_generator/fabric\_workload\_runner.py   | 把实验负载映射成 Fabric 链码调用并执行。                                                                   |
| chain\_backed\_generator/adapter.py                    | Data Adapter，负责 tx\_type 到 contract/function/args 的映射。                                     |
| synthetic\_generator/asset\_hotspot.py                 | 生成热门资产访问负载。                                                                                |
| synthetic\_generator/scene\_crowd.py                   | 生成热门场景拥挤负载。                                                                                |
| synthetic\_generator/reward\_burst.py                  | 生成奖励集中领取和可交换增量更新负载。                                                                        |
| synthetic\_generator/cross\_chain\_asset\_to\_scene.py | 生成资产链到场景链的跨链 multi-stage 负载。                                                               |
| hybrid\_generator/                                     | 从 profile 读取真实分布并放大、加热点、加突发、加跨片/跨链。                                                        |
| profiler/profile\_trace.py                             | 从 trace 中提取交易比例、key popularity、访问集合大小、冲突比例等。                                               |

# **二十一、trace 目录设计**

> trace/
> 
> ├── schema/
> 
> ├── collector/
> 
> ├── converter/
> 
> ├── validator/
> 
> ├── writer/
> 
> └── storage/

|                                       |                                                        |
| ------------------------------------- | ------------------------------------------------------ |
| **文件/目录**                             | **应写模块**                                               |
| schema/tx\_trace.schema.json          | 单链 trace 字段规范。                                         |
| schema/cross\_tx\_trace.schema.json   | 跨链 multi-stage trace 字段规范。                             |
| schema/trace\_meta.schema.json        | trace 统计元信息规范。                                         |
| collector/fabric\_trace\_collector.py | 采集 Fabric 执行结果、事件、延迟和状态访问。                             |
| collector/access\_schema\_loader.py   | 读取链码 access\_schema.yaml，把链码调用转成访问列表。                  |
| converter/raw\_to\_unified.py         | 把原始链上日志转为统一 trace。                                     |
| converter/cross\_stage\_builder.py    | 把跨链多阶段事件拼成 cross\_trace。                               |
| validator/validate\_trace.py          | 校验 read\_set、write\_set、access\_list、字段完整性和 schema 版本。 |
| writer/jsonl\_writer.py               | 写 trace.jsonl 和 cross\_trace.jsonl。                    |
| writer/gzip\_jsonl\_writer.py         | 流式写 trace.jsonl.gz 和 cross\_trace.jsonl.gz。            |
| storage/parquet\_exporter.py          | V2/V3 将 trace 转成 Parquet / Arrow 进行归档和分析。              |
| writer/meta\_writer.py                | 写 trace\_meta.json。                                    |

# **二十二、protocols 目录设计**

> protocols/
> 
> ├── registry/
> 
> ├── consensus\_protocol/
> 
> ├── consensus\_sharding/
> 
> ├── cross\_shard\_protocol/
> 
> └── cross\_chain\_protocol/

|                                                    |                                                                          |
| -------------------------------------------------- | ------------------------------------------------------------------------ |
| **文件/目录**                                          | **应写模块**                                                                 |
| registry/plugin\_base.py                           | 所有插件公共字段：name、type、version、parameters、metrics、validate\_config、maturity。 |
| registry/plugin\_registry.py                       | 注册插件、按类型查询插件、检查插件组合合法性。                                                  |
| registry/plugin\_schema.yaml                       | 插件声明文件 schema。                                                           |
| consensus\_protocol/base.py                        | 共识协议统一接口：order(pending\_txs, cfg) -\> ordered\_blocks + stats。           |
| consensus\_protocol/raft\_like.py                  | leader-based ordering、低延迟、确定性 finality。                                  |
| consensus\_protocol/pbft\_like.py                  | BFT、quadratic message cost、view change。                                  |
| consensus\_protocol/hotstuff\_like.py              | 多阶段确认、linear message cost、rotating leader。                               |
| consensus\_protocol/pow\_like.py                   | block interval、confirmations、fork probability、概率 finality。               |
| consensus\_sharding/single\_group.py               | 单共识组排序。                                                                  |
| consensus\_sharding/beacon\_plus\_shards.py        | beacon + shard committees 模型。                                            |
| cross\_shard\_protocol/two\_phase\_commit\_like.py | 链内跨片 prepare/commit 协调模型。                                                |
| cross\_chain\_protocol/committee\_bridge.py        | 管委会跨链桥流程。                                                                |
| cross\_chain\_protocol/pending\_pool.py            | WaitingMint、WaitingFinality、Completed、Timeout、Refunded 状态管理。             |
| cross\_chain\_protocol/certificate.py              | MintCert、RefundCert、UpdateCert、threshold、生成/验证延迟。                        |
| cross\_chain\_protocol/metaflow\_bridge.py         | MetaFlow 插件：Pending Pool、FDA、批次/窗口协调、多通道。                                |

# **二十三、executor 目录设计**

> executor/
> 
> ├── cmd/run\_experiment/main.go
> 
> ├── core/
> 
> ├── time/
> 
> ├── consensus/
> 
> ├── state\_sharding/
> 
> ├── execution\_sharding/
> 
> ├── routing/
> 
> ├── cross\_shard/
> 
> ├── execution/
> 
> ├── commit/
> 
> ├── state\_store/
> 
> ├── dependency/
> 
> ├── metrics/
> 
> └── utils/

|                                    |                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------ |
| **文件/目录**                          | **应写模块**                                                                 |
| core/transaction.go                | 交易结构：ID、Type、ChainID、ReadSet、WriteSet、AccessList、Commutative、UpdateType。 |
| core/config.go                     | 读取实验 YAML，解析 runtime、system、consensus、routing、execution、commit、metrics。  |
| core/experiment.go                 | 主流程：LoadTrace、InitModules、Order、Routing、Execution、Commit、Metrics。        |
| time/virtual\_clock.go             | 虚拟时钟实现，维护当前逻辑时间。                                                         |
| time/event\_queue.go               | 离散事件队列，按时间戳推进实验。                                                         |
| state\_sharding/interface.go       | LocateState(key) int，对应 phi。                                             |
| execution\_sharding/interface.go   | Assign(tx, ctx) int，对应 psi\_t。                                           |
| routing/interface.go               | BuildRouting(batch, stateMap) -\> RoutingResult，输出 M\_t 和 psi\_t。        |
| routing/coaccess.go                | 构建 X\_t、F\_t、W\_t，根据状态共现进行路由。                                            |
| routing/mt\_routing.go             | 实现批次级访问列表驱动路由。                                                           |
| execution/dual\_track.go           | SplitTracks、fast queue、conservative queue、TopoSafe、非阻塞调度。                |
| commit/hot\_update\_aggregation.go | 识别可交换增量更新、按状态键聚合、约束检查、fallback。                                          |
| dependency/conflict.go             | 读写冲突判断。                                                                  |
| dependency/topo\_safe.go           | 拓扑安全推进判断。                                                                |
| metrics/writer.go                  | 输出 summary.csv、latency.csv、throughput.csv、remote\_wait.csv。              |

# **二十四、experiments 与 metrics 目录设计**

|                                                       |                                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------------ |
| **目录/文件**                                             | **应写模块**                                                           |
| experiments/run.py                                    | 单次实验入口，读取 config 并调用 pipeline。                                     |
| experiments/sweep.py                                  | 批量参数扫描，例如 Zipf、跨片比例、跨链比例、finality\_delay、committee\_size。          |
| experiments/report.py                                 | 读取结果并生成 report.md、图表、LaTeX 表格。                                     |
| experiments/suites/icde\_main.yaml                    | 当前无状态分片论文主实验套件。                                                    |
| experiments/suites/cross\_chain\_main.yaml            | MetaFlow/跨链实验套件。                                                   |
| metrics/prometheus/prometheus.yml                     | Prometheus 采集配置。                                                   |
| metrics/grafana/dashboards/system\_overview.json      | 系统资源 dashboard，采用黑白灰配色。                                            |
| metrics/grafana/dashboards/executor\_metrics.json     | 执行器 TPS、P99、remote\_wait、rollback dashboard。                       |
| metrics/grafana/dashboards/cross\_chain\_metrics.json | 跨链 latency、pending pool、finality wait dashboard。                   |
| metrics/collectors/pending\_pool\_exporter.py         | 输出 pending\_pool\_size、WaitingMint、WaitingFinality、timeout、refund。 |
| metrics/reports/latex\_exporter.py                    | 生成论文用 LaTeX 表格。                                                    |

# **二十五、data、docs 与数据生命周期管理**

|                                           |                             |
| ----------------------------------------- | --------------------------- |
| **目录/文件**                                 | **内容**                      |
| data/raw\_chain\_logs/                    | Fabric / EVM 原始链上日志。        |
| data/traces/                              | 单链 trace.jsonl.gz。          |
| data/cross\_traces/                       | 跨链 cross\_trace.jsonl.gz。   |
| data/trace\_meta/                         | trace\_meta.json。           |
| data/results/{experiment\_id}/config.yaml | 实验实际使用配置。                   |
| data/results/{experiment\_id}/runtime.log | 运行日志。                       |
| data/results/{experiment\_id}/summary.csv | 核心指标汇总。                     |
| data/results/{experiment\_id}/figures/    | 生成图表，全部采用黑白灰配色。             |
| data/results/{experiment\_id}/report.md   | 自动实验报告。                     |
| docs/architecture.md                      | 总体架构说明。                     |
| docs/setup.md                             | 安装与快速启动。                    |
| docs/plugin\_development.md               | 如何新增协议插件。                   |
| docs/trace\_format.md                     | trace 格式说明。                 |
| docs/cross\_chain\_experiments.md         | 异构链、管委会桥、Pending Pool 实验说明。 |
| docs/retention\_policy.md                 | 数据保留、归档、删除和 pinned 规则。      |
| docs/troubleshooting.md                   | 常见错误和解决方案。                  |

## **25.1 数据生命周期策略**

|           |                             |                 |                                         |
| --------- | --------------------------- | --------------- | --------------------------------------- |
| **实验类型**  | **trace**                   | **runtime.log** | **summary/config/report**               |
| Dev/Test  | 7 天后删除或压缩归档。                | 7 天后删除。         | 长期保留 summary.csv、config.yaml、report.md。 |
| Normal    | 30 天后压缩，60 天后删除未 pinned 数据。 | 30 天后压缩。        | 长期保留。                                   |
| Pinned    | 长期保留或迁移至低速硬盘/对象存储。          | 压缩保留。           | 长期保留。                                   |
| Published | 永久归档，保留 schema 和插件版本。       | 压缩归档。           | 永久保留。                                   |

> retention:
> 
> class: dev
> 
> keep\_trace\_days: 7
> 
> keep\_logs\_days: 7
> 
> archive\_after\_days: 30
> 
> delete\_unpinned\_after\_days: 60

# **二十六、CI/CD 与防腐层测试**

由于平台面向课题组长期使用，后续成员会不断修改 Transaction 结构、插件接口和算法实现，因此必须引入 CI/CD 防腐层测试。任何新插件或核心结构修改都应至少通过最小 sanity benchmark，防止破坏已有 baseline。

|                 |                                                              |
| --------------- | ------------------------------------------------------------ |
| **测试类型**        | **检查内容**                                                     |
| 正确性检查           | 交易数、成功率、状态守恒、hot update aggregation 结果、跨片比例统计、summary 字段完整性。 |
| 确定性检查           | 同一 seed、同一 config、同一 trace 运行两次，核心结果保持一致。                    |
| 性能回归检查          | baseline TPS 不应低于设定阈值，P99 不应超过设定阈值；阈值允许机器波动。                 |
| 插件接口检查          | plugin.yaml 是否完整，输入输出是否匹配，兼容性声明是否合法。                         |
| Trace schema 检查 | trace.jsonl.gz 是否可流式读取，schema\_version 是否匹配。                 |

> .github/workflows/sanity.yml
> 
> tests/sanity/v0\_mock\_asset.yaml
> 
> tests/golden/trace\_small.jsonl.gz
> 
> tests/golden/expected\_summary.json
> 
> tests/integration/test\_executor\_replay.py

# **二十七、分阶段建设路线**

|                |                                                                                                                                                                                                                                                                 |                                                                                                 |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| **阶段**         | **建设内容**                                                                                                                                                                                                                                                        | **目标**                                                                                          |
| V0：平台骨架闭环      | 基础前端、后端调度、Experiment Composer、MockChain、AssetHotspot synthetic workload、trace.jsonl.gz、simple\_ordering、single\_group、hash\_state\_sharding、hash\_execution\_sharding、hash\_routing、local\_only、serial\_execution、normal\_commit、virtual\_clock、basic\_metrics。 | 不再单独设置纯命令行原型阶段，而是直接跑通“前端配置 -\> 后端装配 -\> 负载 -\> Trace -\> Executor -\> Metrics -\> 前端结果”的完整平台链路。 |
| V1：支撑当前无状态分片论文 | Fabric Asset/Scene/Reward 链码、access\_schema.yaml、chain-backed trace、混合负载、co-access routing、dual-track、hot update aggregation、Block-STM-like、Calvin-like、Porygon-like baseline、CI sanity。                                                                        | 支撑当前系统论文主实验、baseline 对比和消融实验。                                                                   |
| V2：课题组通用平台     | 更多共识协议、共识分片、跨片协议、跨链协议、Pending Pool、MetaFlow、实验模板、多用户管理、Prometheus/Grafana、数据保留策略、Parquet/Arrow。                                                                                                                                                                 | 支撑分片、共识、跨链、NFT、存储等多方向实验，并允许成员替换单个插件复用其他默认组件。                                                    |
| V3：多服务器真实部署    | 多服务器 Fabric、多组织多 peer、多链跨链、独立监控节点、跨机器网络延迟测量、对象存储归档。                                                                                                                                                                                                             | 增强大规模和真实分布式部署说服力。                                                                               |

开发顺序建议：直接从 V0 平台骨架闭环开始，不再单独设置纯命令行 V0。第一步先搭基础前端、后端、默认插件包、Experiment Composer、虚拟时钟和 trace.jsonl.gz 流式读写，保证每类核心模块至少有一个默认实现并能端到端运行；随后再接 Fabric 链上 trace、论文机制和 baseline；最后扩展跨链、监控、多用户和多服务器部署。

# **二十八、V0 平台骨架闭环文件清单**

|                       |                                                                                                                                                                                                                                                                                                                                                                                                        |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **模块**                | **V0 必须实现的文件**                                                                                                                                                                                                                                                                                                                                                                                         |
| frontend              | pages/Experiments、pages/Composer、pages/RunConsole、pages/Results、components/PluginSelector、api/client.ts、api/websocket.ts。                                                                                                                                                                                                                                                                              |
| backend               | app/main.py、api/experiments.py、api/composer.py、services/experiment\_controller.py、services/experiment\_composer.py、services/compatibility\_checker.py、services/process\_runner.py、services/websocket\_manager.py。                                                                                                                                                                                      |
| configs               | experiments/v0\_default\_asset\_hotspot.yaml、templates/default\_single\_chain\_experiment.yaml、plugins/default\_components.yaml、workloads/asset\_hotspot.yaml、schemas/experiment.schema.json。                                                                                                                                                                                                          |
| chain/mockchain       | chain.py、finality.py。                                                                                                                                                                                                                                                                                                                                                                                  |
| workload              | common/tx\_types.py、common/key\_model.py、common/distributions.py、common/access\_builder.py、synthetic\_generator/main.py、synthetic\_generator/asset\_hotspot.py。                                                                                                                                                                                                                                        |
| trace                 | schema/tx\_trace.schema.json、writer/gzip\_jsonl\_writer.py、writer/meta\_writer.py、validator/validate\_trace.py。                                                                                                                                                                                                                                                                                        |
| protocols/default     | registry/plugin\_registry.py、registry/plugin\_schema.yaml、consensus\_protocol/simple\_ordering.py、consensus\_sharding/single\_group.py、cross\_shard\_protocol/local\_only.py、cross\_chain\_protocol/disabled.py。                                                                                                                                                                                       |
| executor              | core/config.go、core/experiment.go、core/transaction.go、time/virtual\_clock.go、time/event\_queue.go、consensus/simple\_ordering.go、consensus\_sharding/single\_group.go、state\_sharding/hash\_state\_sharding.go、execution\_sharding/hash\_execution\_sharding.go、routing/hash\_routing.go、cross\_shard/local\_only.go、execution/serial\_execution.go、commit/normal\_commit.go、metrics/basic\_metrics.go。 |
| experiments / metrics | experiments/run.py、metrics/writer.go、summary.csv、latency.csv、runtime.log 输出模板。                                                                                                                                                                                                                                                                                                                         |
| tests                 | tests/sanity/v0\_default\_asset\_hotspot.yaml、tests/golden/trace\_small.jsonl.gz、tests/golden/expected\_summary.json、tests/integration/test\_executor\_replay.py。                                                                                                                                                                                                                                      |

V0 的验收标准是：用户可以通过基础前端创建 default\_single\_chain\_experiment，查看系统自动补齐后的默认组件组合，启动实验并实时查看日志；后端能够生成完整 config.yaml 并完成兼容性检查；workload 能生成 asset\_hotspot trace.jsonl.gz；executor 能加载 simple\_ordering、single\_group、hash\_state\_sharding、hash\_execution\_sharding、hash\_routing、local\_only、serial\_execution、normal\_commit、virtual\_clock 和 basic\_metrics；最终输出 summary.csv、latency.csv、runtime.log，并在结果页展示 TPS、P99、成功率和文件下载。同一 seed 重复运行时，核心结果应保持一致。

# **二十九、V1 与 V2 扩展重点**

|        |                                                                                                                                                                                                                                            |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **版本** | **新增重点**                                                                                                                                                                                                                                   |
| V1     | 在 V0 默认链路基础上接入 Fabric 网络、Asset/Scene/Reward 链码、access\_schema.yaml、Fabric trace collector、chain-backed workload、co-access routing、dual\_track.go、hot\_update\_aggregation.go、Block-STM-like、Calvin-like、Porygon-like baseline 和 CI sanity。 |
| V2     | 在 V1 论文实验能力基础上扩展共识协议、共识分片、跨片协议、跨链协议、committee\_bridge、pending\_pool、metaflow\_bridge、TopologyDesigner、CrossChainProtocolPanel、PendingPoolPanel、Grafana dashboard、数据保留策略和 Parquet/Arrow。                                                    |

# **三十、风险与控制建议**

|                 |                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------- |
| **风险**          | **控制方法**                                                                                    |
| 工程量过大           | 严格按 V0 -\> V1 -\> V2 -\> V3 分阶段建设，不要一开始全做。                                                  |
| 普通电脑跑不动         | 普通电脑只跑 Local Dev Mode，正式实验放到共享服务器。                                                          |
| 插件组合混乱          | 每个插件提供 plugin.yaml，前端初检，后端强校验，Experiment Composer 自动补齐默认组件。                                 |
| 实验不可复现          | 保存 seed、config.yaml、trace\_meta、插件版本、日志、结果和 schema\_version。                                |
| 真实链和模拟层口径混淆     | 明确真实链生成 chain-backed trace，模块化实验层做可控对比。                                                     |
| 跨链实验过于复杂        | 第一版先做 Mock 双链 + committee bridge + pending pool，再接 Fabric 双链。                               |
| 指标不统一           | 所有 baseline 统一输出 summary.csv、latency.csv、throughput.csv、cross\_chain.csv、pending\_pool.csv。 |
| 物理时钟污染延迟        | executor 中采用 virtual\_time / DES，不用 time.Sleep 模拟网络和 finality 延迟。                           |
| Trace I/O 瓶颈    | V0/V1 使用 jsonl.gz 流式读写，V2/V3 支持 Parquet / Arrow。                                            |
| 共享服务器爆盘         | 按 Dev/Test、Normal、Pinned、Published 配置保留策略和归档策略。                                             |
| 后续修改破坏 baseline | 通过 GitHub Actions / GitLab CI 运行 sanity benchmark 和 golden trace 测试。                        |

# **三十一、最终总结**

本平台最终应定位为一个“链上业务语义 trace + 可控模拟负载 + 插件化协议实验 + 实验装配 + 虚拟时钟重放 + 可视化管理 + 工程化治理”的区块链元宇宙实验平台。它既能支撑当前无状态分片执行论文，也能支撑后续 MetaFlow 异构链跨链实验，并能让课题组成员在统一框架下复用已有组件、接入自己的算法插件和生成可比较的实验结果。

当前最优先的落地目标是直接完成 V0 平台骨架闭环：基础前端 -\> 后端配置生成 -\> Experiment Composer 默认组件补齐 -\> AssetHotspot workload -\> trace.jsonl.gz -\> virtual-time executor replay -\> basic\_metrics -\> 前端结果展示。V0 不追求算法丰富度，每类插件只实现一个最基础版本；后续再逐步扩展到 Fabric trace、hybrid workload、co-access routing、dual-track/hot aggregation、committee bridge、Pending Pool、CI/CD、数据保留策略和完整 Web 平台。
