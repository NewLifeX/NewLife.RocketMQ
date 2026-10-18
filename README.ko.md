# NewLife.RocketMQ - 엔터프라이즈 순수 관리형 RocketMQ 클라이언트

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.RocketMQ?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.RocketMQ?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.RocketMQ?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.RocketMQ?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.RocketMQ/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.RocketMQ?logo=github)](https://github.com/NewLifeX/NewLife.RocketMQ/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [العربية](README.ar.md)
<!-- lang:end -->

> 이 번역은 중국어 버전보다 늦을 수 있습니다.

NewLife.RocketMQ는 순수 관리형 RocketMQ 클라이언트로 Apache RocketMQ와 Alibaba Cloud를 지원하며 일반·순서·트랜잭션·지연 메시지를 처리합니다.

## 설치

```bash
dotnet add package NewLife.RocketMQ
```

## 빠른 시작

```csharp
var mq = new RocketMQClient("127.0.0.1:9876");
mq.Start();
mq.Send("TopicTest", "hello".GetBytes());
```

## 주요 기능

- 주요 프로토콜과 형식 버전을 폭넓게 지원
- 주요 클라우드 서비스와 관리형 미들웨어 지원
- 장기 운영 환경에서 검증됨

## 문서

> 📖 전체 문서(중국어): [저장소 문서](Doc)

## 라이선스

MIT License © 2002-2026 NewLife 팀

[GitHub](https://github.com/NewLifeX/NewLife.RocketMQ) | [Website](https://newlifex.com)
