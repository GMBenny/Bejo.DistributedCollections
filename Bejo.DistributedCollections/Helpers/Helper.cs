using System.IO.Compression;
using System.Security.Cryptography;

namespace Bejo.DistributedCollections.Helpers;


internal static class Helper
{
    internal static ulong Hash(string data)
    {
        return Hash(System.Text.Encoding.UTF8.GetBytes(data));
    }

    internal static ulong Hash(byte[] data)
    {
        ulong hash = 0;

        for (int i = 0; i < data.Length; i++)
        {
            var b = data[i];

            ulong tmp = 0;
            if (i % 8 == 0)
                tmp = (ulong)b << 56;
            else if (i % 8 == 1)
                tmp = (ulong)b << 48;
            else if (i % 8 == 2)
                tmp = (ulong)b << 40;
            else if (i % 8 == 3)
                tmp = (ulong)b << 32;
            else if (i % 8 == 4)
                tmp = (ulong)b << 24;
            else if (i % 8 == 5)
                tmp = (ulong)b << 16;
            else if (i % 8 == 6)
                tmp = (ulong)b << 8;
            else if (i % 8 == 7)
                tmp = (ulong)b;

            hash ^= tmp;

        }
        return hash;
    }

    internal static byte[] Encrypt(byte[] data, string password)
    {
        using var aes = Aes.Create();
        var salt = RandomNumberGenerator.GetBytes(16);
        var encryptionKey = Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            100_000,
            HashAlgorithmName.SHA256,
            32);
        aes.Key = encryptionKey;
        aes.GenerateIV();
        using var ms = new MemoryStream();
        ms.Write(salt, 0, salt.Length);
        ms.Write(aes.IV, 0, aes.IV.Length);
        using (var cs = new CryptoStream(ms, aes.CreateEncryptor(), CryptoStreamMode.Write))
        {
            cs.Write(data, 0, data.Length);
            cs.FlushFinalBlock();
        }
        return ms.ToArray();
    }
    internal static byte[]? Decrypt(byte[] data, string password)
    {
        try
        {
            using var aes = Aes.Create();
            using var ms = new MemoryStream(data);
            var salt = new byte[16];
            ms.Read(salt, 0, salt.Length);
            var iv = new byte[aes.BlockSize / 8];
            ms.Read(iv, 0, iv.Length);
            var encryptionKey = Rfc2898DeriveBytes.Pbkdf2(
                password,
                salt,
                100_000,
                HashAlgorithmName.SHA256,
                32);
            aes.Key = encryptionKey;
            aes.IV = iv;
            using var cs = new CryptoStream(ms, aes.CreateDecryptor(), CryptoStreamMode.Read);
            using var output = new MemoryStream();
            cs.CopyTo(output);
            return output.ToArray();
        }
        catch (Exception)
        {
            return null;
        }
    }

    internal static byte[] Compress(byte[] data)
    {
        MemoryStream output = new MemoryStream();
        using (DeflateStream dstream = new DeflateStream(output, CompressionLevel.Optimal))
        {
            dstream.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    internal static byte[]? Decompress(byte[] data)
    {
        try
        {
            MemoryStream input = new MemoryStream(data);
            MemoryStream output = new MemoryStream();
            using (DeflateStream dstream = new DeflateStream(input, CompressionMode.Decompress))
            {
                dstream.CopyTo(output);
            }
            return output.ToArray();
        }
        catch (Exception)
        {
            return null;
        }
    }
}