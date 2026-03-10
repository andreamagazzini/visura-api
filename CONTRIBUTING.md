# Come contribuire a Visura API

Grazie per il tuo interesse nel contribuire a Visura API! Questo documento fornisce le linee guida per partecipare al progetto.

> **Regola fondamentale**: prima di scrivere codice, **apri sempre una issue** per discutere la modifica. Le PR senza issue collegata e approvata non verranno revisionate. Questo vale per feature, refactoring e fix non banali.

## Tempi di risposta

Questo progetto è mantenuto nel tempo libero. Ecco cosa aspettarsi:

- **Issue**: risposta entro 7 giorni
- **PR con issue approvata**: prima review entro 14 giorni
- **PR senza issue**: chiusa con richiesta di aprire prima una issue

Se non ricevi risposta entro questi tempi, sentiti libero di fare un ping gentile nel thread.

## Codice di condotta

Partecipando a questo progetto accetti di rispettare il nostro [Codice di Condotta](CODE_OF_CONDUCT.md).

## Policy sull'uso di strumenti AI

L'uso di strumenti AI (GitHub Copilot, ChatGPT, Claude, ecc.) come **supporto** è benvenuto, ma con regole chiare:

- **Dichiaralo**: nel template della PR c'è una sezione apposita. Sii trasparente su cosa è stato generato da AI.
- **Comprendi il codice**: devi essere in grado di spiegare ogni riga della tua PR. Se il reviewer ti chiede "perché hai fatto X?" e la risposta è "l'ha scritto l'AI", la PR verrà rifiutata.
- **Testa tutto**: il codice generato da AI deve essere testato localmente end-to-end, esattamente come il codice scritto a mano.
- **No bulk PR**: le PR che riformattano massivamente il codice o aggiungono centinaia di righe non richieste non verranno revisionate. Mantieni le PR piccole e focalizzate.

Le PR palesemente generate da AI senza comprensione del progetto (selettori sbagliati, flussi non testati, dipendenze inventate) verranno chiuse senza review.

## Come segnalare un problema

1. Controlla prima le [issue esistenti](https://github.com/zornade/visura-api/issues) per assicurarti che il problema non sia già stato segnalato
2. Se è un problema di sicurezza, **NON** aprire una issue pubblica — segui le istruzioni in [SECURITY.md](SECURITY.md)
3. Apri una nuova issue usando il template appropriato (Bug Report o Feature Request)

## Come proporre modifiche

### Regola: Issue prima, PR dopo

1. **Apri una issue** descrivendo cosa vuoi fare e perché
2. **Attendi il via libera** del maintainer (etichetta `approved` o commento esplicito)
3. **Solo dopo**, crea il fork e scrivi il codice

Questo evita di sprecare il tuo tempo su modifiche che non verranno accettate e permette di allinearsi sull'architettura prima di scrivere codice.

### Preparare l'ambiente di sviluppo

```bash
# Clona il repository
git clone https://github.com/zornade/visura-api.git
cd visura-api

# Crea un ambiente virtuale
python -m venv .venv
source .venv/bin/activate

# Installa le dipendenze (incluse quelle di sviluppo)
pip install -r requirements.txt
pip install pytest pytest-cov black ruff

# Installa Playwright
playwright install chromium

# Copia il file di esempio per le variabili d'ambiente
cp .env.example .env
# Modifica .env con le tue credenziali
```

### Flusso di lavoro

1. **Crea un fork** del repository
2. **Crea un branch** dal `main` con un nome descrittivo:
   ```bash
   git checkout -b fix/correzione-login
   git checkout -b feat/nuova-funzionalita
   ```
3. **Scrivi il codice** seguendo le convenzioni del progetto
4. **Aggiungi test** per le modifiche, se applicabile
5. **Verifica linting e formattazione** prima del commit:
   ```bash
   black --check .
   ruff check .
   ```
6. **Esegui i test** per verificare che nulla sia rotto:
   ```bash
   python -m pytest test_*.py -v
   ```
7. **Fai rebase su main** prima di aprire la PR:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```
8. **Fai commit** con messaggi chiari in italiano:
   ```bash
   git commit -m "fix: corretto errore nella selezione della provincia"
   git commit -m "feat: aggiunto supporto per ricerca per codice fiscale"
   ```
9. **Apri una Pull Request** verso il branch `main`, compilando il template e collegando la issue

> ⚡ La CI eseguirà automaticamente linting (ruff + black), test e un audit di sicurezza sulle dipendenze. Assicurati che passi tutto prima di chiedere la review.

### Convenzioni per i messaggi di commit

Usiamo il formato [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` — nuova funzionalità
- `fix:` — correzione di un errore
- `docs:` — modifiche alla documentazione
- `refactor:` — ristrutturazione del codice senza cambi funzionali
- `test:` — aggiunta o modifica di test
- `ci:` — modifiche alla configurazione CI/CD

### Stile del codice

- Seguiamo le convenzioni **PEP 8**
- Usa **Black** per la formattazione automatica: `black .`
- Usa **Ruff** per il linting: `ruff check .`
- Aggiungi type hints dove possibile
- Documenta le funzioni pubbliche con docstring
- Commenti e log in **italiano**

### Cosa NON committare mai

- File `.env` con credenziali reali
- Log con dati personali (codici fiscali, nomi, ecc.)
- File temporanei di debug (`/tmp/debug_*.html`)
- Cartelle di cache (`__pycache__/`, `.venv/`)

## Aree dove servono contributi

- Miglioramento dei test automatici
- Documentazione (README, guide, esempi)
- Gestione degli errori e resilienza
- Ottimizzazione delle performance
- Supporto per nuove funzionalità del portale SISTER

## Domande?

Apri una [discussione](https://github.com/zornade/visura-api/discussions) o una issue con l'etichetta `domanda`.

Grazie per il tuo contributo!
