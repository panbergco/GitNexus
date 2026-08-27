import path from 'node:path';
import { getGitRoot } from '../storage/git.js';
import { getStoragePaths, loadMeta, saveMeta } from '../storage/repo-manager.js';
import {
  closeLbug,
  executeQuery,
  executeWithReusedStatement,
  initLbug,
  loadCachedEmbeddings,
} from '../core/lbug/lbug-adapter.js';
import { EMBEDDING_TABLE_NAME } from '../core/lbug/schema.js';
import { runEmbeddingPipeline } from '../core/embeddings/embedding-pipeline.js';

/** Add missing embeddings directly to a healthy index, checkpointing every batch. */
export const embeddingsCommand = async (inputPath?: string): Promise<void> => {
  const repoPath = inputPath ? path.resolve(inputPath) : getGitRoot(process.cwd());
  if (!repoPath) throw new Error('Not inside a git repository. Pass a repository path.');

  const { lbugPath, metaPath } = getStoragePaths(repoPath);
  const metaDir = path.dirname(metaPath);
  const meta = await loadMeta(metaDir);
  if (!meta) throw new Error(`No GitNexus index found for ${repoPath}. Run gitnexus analyze first.`);
  if (meta.incrementalInProgress) {
    throw new Error('The structural index is incomplete. Run gitnexus analyze --force first.');
  }

  await initLbug(lbugPath);
  try {
    const cached = await loadCachedEmbeddings();
    const existing = new Map(
      cached.embeddings.map((row) => [row.nodeId, row.contentHash ?? '']),
    );
    let lastPercent = -1;

    console.log(`Embedding ${repoPath}`);
    console.log(`Checkpointed vectors already present: ${cached.embeddings.length}`);

    await runEmbeddingPipeline(
      executeQuery,
      executeWithReusedStatement,
      (progress) => {
        const percent = Math.floor(progress.percent);
        if (percent !== lastPercent && (percent % 5 === 0 || percent === 100)) {
          lastPercent = percent;
          console.log(
            `  ${percent}% — ${progress.nodesProcessed ?? 0}/${progress.totalNodes ?? '?'} nodes`,
          );
        }
      },
      {},
      undefined,
      existing.size ? existing : undefined,
    );

    const rows = await executeQuery(
      `MATCH (e:${EMBEDDING_TABLE_NAME}) RETURN count(e) AS cnt`,
    );
    const embeddings = Number(rows?.[0]?.cnt ?? rows?.[0]?.[0] ?? 0);
    await saveMeta(metaDir, {
      ...meta,
      stats: { ...meta.stats, embeddings },
    });
    console.log(`Embeddings ready: ${embeddings}`);
  } finally {
    await closeLbug();
  }
};
