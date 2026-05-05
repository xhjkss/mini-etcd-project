package com.xhj.etcd.storage.file;

import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.StorageException;
import com.xhj.etcd.storage.StorageKey;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/**
 * FileStorage
 *
 * @author XJks
 * @description 文件存储实现，负责把 Storage 层的 group/key 定位模型映射为本地文件路径。
 * <p>
 * TODO:
 *  StorageKey 只表达 Storage 层的存储定位语义，不承载上层业务含义。
 *  在 FileStorage 中，group 会被映射为根目录下的一级子目录，key 会被映射为该目录下的数据文件。
 *  <pre>
 *      StorageKey(group, key)
 *              |
 *              v
 *      rootDir / encode(group) / encode(key).data
 *  </pre>
 */
public class FileStorage implements Storage {

    /**
     * 存储文件后缀。
     *
     * <p>FileStorage 只把带有该后缀的文件识别为真实数据文件，
     * 临时文件、中间文件或其他杂项文件不会被 listKeys 读取。</p>
     */
    private static final String DATA_FILE_SUFFIX = ".data";

    /**
     * 文件存储根目录。
     *
     * <p>所有数据都会落在该目录下，但不会直接平铺存储；
     * FileStorage 会先根据 group 创建一级子目录，再根据 key 创建具体数据文件。</p>
     */
    private final Path rootDir;

    public FileStorage(File rootDir) {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir must not be null");
        }
        this.rootDir = rootDir.toPath();
        ensureDirectory(this.rootDir);
    }

    public FileStorage(Path rootDir) {
        if (rootDir == null) {
            throw new IllegalArgumentException("rootDir must not be null");
        }
        this.rootDir = rootDir;
        ensureDirectory(this.rootDir);
    }

    // ==================== Write ====================

    @Override
    public void put(String group, String key, byte[] value) {
        // TODO:StorageKey 在这里统一校验 group/key，并明确本次写入要定位到哪一个存储槽位。
        StorageKey storageKey = new StorageKey(group, key);

        if (value == null) {
            delete(group, key);
            return;
        }

        try {
            // group 决定一级目录，相同 key 在不同 group 下会落到不同目录，因此不会互相覆盖。
            Path groupDir = groupDir(storageKey.getGroup());
            ensureDirectory(groupDir);

            // key 决定当前 group 目录下的数据文件名。
            Path targetFile = dataFile(storageKey.getGroup(), storageKey.getKey());

            // 先写临时文件，写入完成后再替换正式文件，避免写入过程中读到半截数据。
            Path tempFile = groupDir.resolve(encode(storageKey.getKey()) + ".tmp");

            Files.write(tempFile, Arrays.copyOf(value, value.length));
            try {
                // 优先使用原子移动，保证同一个 key 对应的数据文件要么是旧值，要么是新值。
                Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception atomicMoveFailed) {
                // 部分文件系统不支持 ATOMIC_MOVE，退化为普通替换。
                Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception e) {
            throw new StorageException("file storage put failed, group=" + group + ", key=" + key, e);
        }
    }

    // ==================== Read ====================

    @Override
    public byte[] get(String group, String key) {
        // StorageKey 只描述底层存储定位：在哪个 group 下读取哪个 key。
        StorageKey storageKey = new StorageKey(group, key);

        try {
            // 根据 group/key 还原到 FileStorage 的真实数据文件路径。
            Path file = dataFile(storageKey.getGroup(), storageKey.getKey());
            if (!Files.exists(file)) {
                return null;
            }
            byte[] value = Files.readAllBytes(file);

            // 返回副本，避免调用方修改返回数组后影响 Storage 内部语义。
            return Arrays.copyOf(value, value.length);
        } catch (Exception e) {
            throw new StorageException("file storage get failed, group=" + group + ", key=" + key, e);
        }
    }

    // ==================== Delete ====================

    @Override
    public void delete(String group, String key) {
        // 删除操作和读写操作使用同一套 group/key 到文件路径的映射规则。
        StorageKey storageKey = new StorageKey(group, key);

        try {
            Files.deleteIfExists(dataFile(storageKey.getGroup(), storageKey.getKey()));
        } catch (Exception e) {
            throw new StorageException("file storage delete failed, group=" + group + ", key=" + key, e);
        }
    }

    // ==================== List ====================

    @Override
    public List<String> listKeys(String group) {
        if (group == null || group.trim().length() == 0) {
            throw new IllegalArgumentException("group must not be empty");
        }

        try {
            // listKeys 只在一个 group 内枚举 key，因此这里只需要定位 group 目录。
            Path groupDir = groupDir(group);
            if (!Files.exists(groupDir)) {
                return new ArrayList<>();
            }

            List<String> result = new ArrayList<>();
            File[] files = groupDir.toFile().listFiles();
            if (files == null) {
                return result;
            }
            for (File file : files) {
                String fileName = file.getName();

                // 只识别正式数据文件，忽略 .tmp 等临时文件。
                if (!file.isFile() || !fileName.endsWith(DATA_FILE_SUFFIX)) {
                    continue;
                }

                // 文件名是 encode(key) + .data，这里去掉后缀后再 decode 回原始 key。
                String encodedKey = fileName.substring(0, fileName.length() - DATA_FILE_SUFFIX.length());
                result.add(decode(encodedKey));
            }
            Collections.sort(result);
            return result;
        } catch (Exception e) {
            throw new StorageException("file storage list keys failed, group=" + group, e);
        }
    }

    // ==================== Path helpers ====================

    /**
     * 将 Storage 层的 group 映射为本地一级目录。
     * <p>
     * TODO: group 是 FileStorage 的命名空间边界，相同 key 只要 group 不同，就会落到不同目录下的数据文件。
     */
    private Path groupDir(String group) {
        return rootDir.resolve(encode(group));
    }

    /**
     * 将 Storage 层的 group/key 映射为具体数据文件。
     *
     * <p>映射关系为：rootDir / encode(group) / encode(key).data。</p>
     */
    private Path dataFile(String group, String key) {
        return groupDir(group).resolve(encode(key) + DATA_FILE_SUFFIX);
    }

    /**
     * 将 group/key 编码为安全文件名。
     *
     * <p>group/key 可能包含斜杠、空格、特殊字符等，直接作为目录名或文件名容易破坏路径结构；
     * 因此 FileStorage 统一使用 URL-safe Base64 编码。</p>
     */
    private String encode(String text) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 将文件名中的编码内容还原为原始 group/key。
     */
    private String decode(String text) {
        return new String(Base64.getUrlDecoder().decode(text), StandardCharsets.UTF_8);
    }

    /**
     * 确保存储目录存在。
     *
     * <p>rootDir 和 groupDir 都通过该方法创建，避免写入文件时目录不存在。</p>
     */
    private void ensureDirectory(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            throw new StorageException("create storage directory failed: " + dir, e);
        }
    }
}